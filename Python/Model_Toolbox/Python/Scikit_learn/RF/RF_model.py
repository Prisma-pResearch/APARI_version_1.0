# -*- coding: utf-8 -*-
"""
Module to fit randomr forest classifier(s) on a dataset.

Created on Sat Mar 19 09:09:11 2022

@author: ruppert20
"""
from ...DataSet import Dataset
import warnings
import joblib
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from sklearn.feature_selection import SelectKBest, f_classif  # , RFECV
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics
import pandas as pd
import numpy as np
import os
from ...Utilities.Logging.log_messages import log_print_email_message as logm
from ...Utilities.FileHandling.io import save_data
from pytorch_lightning import seed_everything
from tqdm import tqdm
seed_everything(42, workers=True)


def fitRandomForestClassifiers(development: Dataset,
                               test: Dataset,
                               dir_dict: dict,
                               log_name: str = 'Fit_Random_Forest',
                               dset_keys: list = None,
                               filter_features: bool = True,
                               max_cores: int = 8):
    logm('Parameter tunning and feature selection for RandomForestClassifier: ', display=True, log_name=log_name)

    X_train, y_train = development.dataset_as_pandas(keys=dset_keys)
    X_test, y_test = test.dataset_as_pandas(keys=dset_keys)

    logm(message=f'X_train shape: {X_train.shape}', display=True, log_name=log_name)
    logm(message=f'X_train columns: {list(X_train.columns)}', display=True, log_name=log_name)

    for col in tqdm(y_train.columns, desc=log_name):
        logm(message=f'Modeling: {col}', display=True, log_name=log_name)

        pipe_list: list = []
        if filter_features:
            # ANOVA F-value between label/feature for classification tasks (F-test captures only linear dependency)
            kbest = SelectKBest(f_classif)
            pipe_list.append(('kbest', kbest))

        # Create a RF classifier:
        # The “balanced” mode uses the values of y to automatically adjust weights inversely proportional to class frequencies in the input data

        clf = RandomForestClassifier(n_jobs=min(os.cpu_count(), max_cores), random_state=42, class_weight='balanced')

        pipe_list.append(('rf', clf))

        pipeline = Pipeline(pipe_list)

        # param grid:  must have two underscores to split rf and the parameter variable
        param_grid = {'rf__n_estimators': [1000, 1300, 1500, 1700, 2000],
                      'rf__min_samples_leaf': [1, 3, 5, 9, 10],
                      'rf__max_features': ['auto', 'log2', 15, 20, 25, 30, 40, 50]}

        if filter_features:
            param_grid['kbest__k'] = [30, 50, 100, 110, 'all']

        logm(message='--- Initiating grid search ---', display=True, log_name=log_name)
        CV_rfc = GridSearchCV(pipeline, param_grid=param_grid, scoring='roc_auc', cv=5, verbose=2)
        warnings.simplefilter(action='ignore', category=UserWarning)
        warnings.simplefilter(action='ignore', category=RuntimeWarning)
        CV_rfc.fit(X_train, y_train[col])

        logm(message=CV_rfc.best_params_, log_name=log_name)   # log the best parameters

        # perform preditction using the best RF model
        predicted_y = CV_rfc.predict(X_test)
        probs = CV_rfc.predict_proba(X_test)
        probs = probs[:, 1]

        # generate some evaluation metrics
        logm(message=f'--- RF {col} Results ---', display=True, log_name=log_name)
        logm(message=f'ROC-AUC of the RF model with selected features: {metrics.roc_auc_score(y_test[col], probs)}', display=True, log_name=log_name)
        logm(message=f'ROC-AUPRC of the RF model with selected features: {metrics.average_precision_score(y_test[col], probs)}', display=True, log_name=log_name)
        logm(message=f'F1-Score of the RF model with selected features: {metrics.f1_score(y_test[col], predicted_y)}', display=True, log_name=log_name)

        # identify final features
        finalFeatureIndices = CV_rfc.best_estimator_.named_steps['kbest'].get_support(indices=True)
        final_columns = X_train.iloc[:, finalFeatureIndices].columns

        # record feature names in GridSearch Object
        CV_rfc.original_feature_list = X_train.columns.tolist()
        CV_rfc.final_selected_feature_list = final_columns.tolist()
        CV_rfc.target_label = col

        # save weights
        save_data(df=pd.DataFrame({'features': CV_rfc.final_selected_feature_list,
                                   'weight': CV_rfc.best_estimator_.named_steps['kbest'].scores_[finalFeatureIndices]})
                  .sort_values('weight', ascending=False),
                  out_path=os.path.join(dir_dict.get('RF_Model_Results'), f'RF_Feature_Weights_{col}.csv'))

        # save model
        joblib_file: str = os.path.join(dir_dict.get('RF_Model_Results'), f'RF_Model_{col}.pkl')
        logm(message=f'Saving Model to {joblib_file}', display=False, log_name=log_name)
        joblib.dump(CV_rfc, joblib_file)

        # save importances
        forest = CV_rfc.best_estimator_.named_steps['rf']
        importances = forest.feature_importances_
        indices = np.argsort(importances)[::-1]
        save_data(df=pd.DataFrame({'features': np.array(final_columns[indices]), 'importance': importances[indices]}),
                  out_path=os.path.join(dir_dict.get('RF_Model_Results'), f'RF_Feature_Importances_{col}.csv'))

        # save predictions
        save_data(df=pd.DataFrame({f'y_true_{col}': y_test[col],
                                   f'y_pred_{col}': probs},
                                  index=y_test.index),
                  out_path=os.path.join(dir_dict.get('RF_Model_Results'), f'RF_Feature_Predictions_{col}.csv'))


if __name__ == '__main__':
    from Utils.log_messages import start_logging_to_file
    dirpath: str = "path to save data"
    start_logging_to_file(directory=dirpath, file_name=None)
    from Utils.io import check_load_df
    td: dict = check_load_df('config_path')

    dset = Dataset(cohort='Development|Test_UF', **td.get('dataset'))

    fitRandomForestClassifiers(development=dset,
                               test=dset,
                               filter_features=False,
                               dir_dict={'RF_Model_Results': dirpath}, log_name='MSR_MR_Preop',
                               # dset_keys=['preoperative_static_continuous', 'preoperative_static_continuous_indicators', 'preoperative_static_binary'],
                               max_cores=20)
