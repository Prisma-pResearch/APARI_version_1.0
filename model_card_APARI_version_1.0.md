
# Model Card for APARI version 1.0

#  Table of Contents

- [Model Details](#model-details)
  - [Model Description](#model-description)
- [Uses](#uses)
  - [Direct Use](#direct-use)
  - [Downstream Use [Optional]](#downstream-use-optional)
  - [Out-of-Scope Use](#out-of-scope-use)
- [Bias, Risks, and Limitations](#bias-risks-and-limitations)
  - [Recommendations](#recommendations)
- [Training Details](#training-details)
  - [Training Data](#training-data)
  - [Training Procedure](#training-procedure)
    - [Preprocessing](#preprocessing)
    - [Speeds, Sizes, Times](#speeds-sizes-times)
- [Evaluation](#evaluation)
  - [Testing Data, Factors & Metrics](#testing-data-factors--metrics)
    - [Testing Data](#testing-data)
    - [Factors](#factors)
    - [Metrics](#metrics)
  - [Results](#results)
- [Model Examination](#model-examination)
- [Environmental Impact](#environmental-impact)
- [Technical Specifications [optional]](#technical-specifications-optional)
  - [Model Architecture and Objective](#model-architecture-and-objective)
  - [Compute Infrastructure](#compute-infrastructure)
    - [Hardware](#hardware)
    - [Software](#software)
- [Citation](#citation)
- [Glossary [optional]](#glossary-optional)
- [More Information [optional]](#more-information-optional)
- [Model Card Authors [optional]](#model-card-authors-optional)
- [Model Card Contact](#model-card-contact)
- [How to Get Started with the Model](#how-to-get-started-with-the-model)

# Model Details

## Model Description

The APARI (Aligning Patient Acuity with Resource Intensity) model generates predictions of in-hospital mortality and prolonged (greater than 48h) intensive care unit (ICU) stay for patients who are undergoing major surgery. Features include electronic health record (EHR) information available before and during surgery. Model predictions are used to identify high-acuity patients who are planned for postoperative admission to low-acuity hospital floor units and low-acuity patients who are planned for postoperative admission to high-acuity ICUs, and provides decision support for doctors to rethink the triage destination.

- **Developed by:** Matthew M Ruppert, Die Hu, Benjamin Shickel, Tyler J Loftus
- **Funded by:** National Institutes of Health (NIH), National Institute of General Medical Sciences (NIGMS)
- **Model type:** tabular-regression
- **Language(s) (NLP):** eng
- **License:** gpl
- **Patents:**
    - SYSTEMS AND METHODS FOR USING DEEP LEARNING TO GENERATE ACUITY SCORES FOR CRITICALL ILL OR INJURED PATIENTS.
Patent No.: US 12,340,905 B2.
Date of Patent: Jun. 24, 2025.
Inventors: Azra Bihorac, Gainesville, FL (US);
Tyler J. Loftus, Gainesville, FL (US);
Tezcan Ozrazgat Baslanti, Gainesville, FL (US); 
Parisa Rashidi, Gainesville, FL (US); 
Benjamin P. Shickel, Alachua, FL (US).
Assignee: University of Florida Research
Foundation, Incorporated, Gainesville, FL (US).
    - The APARI model evolved from The MySurgeryRisk model (view Model card at: http://10.14.133.29:38010/Doc version of model card), which is further described by: 
METHOD AND APARATUS FOR PREDICTION OF COMPLICATIONS AFTER SURGERY.
Patent No.: US 12,014,832 B2.
Date of Patent: Jun. 18, 2024.
Inventors: Azra Bihorac (US);
Xiaolin Li (US);
Parisa Rashidi (US);
Panagote Pardalos (US);
Tezcan Ozrazgat Baslanti (US); 
William Hogan (US);
Daisy Zhe Wang (US);
Petar Momvilovic (US);
Gloria Lipori (US).
Assignee: University of Florida Research
Foundation, Incorporated, Gainesville, FL (US).
- **Resources for more information:**
    - [GitHub Repo](https://github.com/Prisma-pResearch/APARI_version_1.0)
    - [Associated Paper](Loftus TJ, Ruppert MM, Shickel B, Ozrazgat-Baslanti T, Balch JA, Hu D, Javed A, Madbak F, Skarupa DJ, Guirgis F, Efron PA, Tighe PJ, Hogan WR, Rashidi P, Upchurch GR Jr, Bihorac A. Overtriage, Undertriage, and Value of Care after Major Surgery: An Automated, Explainable Deep Learning-Enabled Classification System. J Am Coll Surg. 2023 Feb 1;236(2):279-291. doi: 10.1097/XCS.0000000000000471. Epub 2022 Nov 8. PMID: 36648256; PMCID: PMC9993068.)

# Uses

## Direct Use

Generating predictions of in-hospital mortality and prolonged (48h or greater) ICU stay among adult (age 18 years or greater) patients who are undergoing major surgery.

## Downstream Use 

Model predictions are used to identify high-acuity patients who are planned for postoperative admission to low-acuity hospital floor units and low-acuity patients who are planned for postoperative admission to high-acuity ICUs, and provides decision support for doctors to rethink the triage destination.

## Out-of-Scope Use

Outpatient operations, hospital admissions lasting less than 24 hours, operations performed on children (age less than 18 years).

# Bias, Risks, and Limitations

Some patients may require ICU admission for frequent neurovascular examinations or continuous invasive monitoring, despite low risk for prolonged ICU admission or hospital mortality; these patients may be more likely to be classified as overtriaged. Some pertinent variables, like free text descriptions of estimated blood loss in operative reports or nursing communications for frequent neurovascular checks, remain unavailable for analysis until we implement validated natural language processing methods. These variables, along with others that are difficult to measure or model in retrospective EHR data (e.g., hospital bed control dynamics, important indicators of patient physiologic status that were unmeasured due to cognitive or systems-based errors), allow for residual confounding in identifying risk-matched controls; mitigating these sources of confounding would require an experimental trial design with randomization. In addition, retrospective data is unaffected by delayed clinical documentation (e.g., documenting comorbidities present on admission), which could degrade real-time model performance. Some potential informative features (e.g., pulmonary function testing) were not available for modeling but could affect the distributions of triage classifications. Diagnostic uncertainty may render ICU admission appropriate for a patient who has low risk for prolonged critical illness or death, and diagnostic uncertainty is not represented explicitly in our postoperative triage classification framework. 

## Recommendations

Human-in-the-loop implementation, retraining on local data, prospective, multi-center validation.

# Training Details

## Training Data

The model was trained on a development cohort of 97,748 patients who had 125,035 admissions that included an inpatient operation at three hospitals within a single healthcare system. Prior to testing on a holdout sample, the model was validated using data from 14,318 patients who had 15,673 admissions that included an inpatient operation. Data obtained from Electronic Health Records (EHRs) that was considered for inclusion in risk stratification included demographics, insurance information, 9-digit zip codes for conversion to area deprivation index state and national rank values, preoperative comorbidities per International Classification of Diseases (ICD) codes, Charlson comorbidity indices, smoking status, American Society of Anesthesiologists (ASA) physical status classification scores, sequential organ failure assessment (SOFA) scores at the time of admission and immediately prior to surgery (calculated by a Python module generated by the authors which assays all information necessary to calculate SOFA scores from EHR data and generates component and total scores using data for which timestamps are within investigator-determined time windows), whether the patient was admitted from the emergency department, laboratory values, vital signs, the primary surgery service for the index operation, primary procedure codes (current procedural terminology, CPT) for the index operation, the primary surgeon represented as a deidentified categorical variable, admission and surgery priority classes (elective, urgent, or emergent), and intraoperative physiologic data including heart rate, systolic and diastolic blood pressure, body temperature, respiratory rate, positive end-expiratory pressure, peak inspiratory pressure, tidal volume, fraction of inspired oxygen, blood oxygen saturation, and end-tidal carbon dioxide. Patient characteristics and outcomes are similar to those summarized in the "Testing Data" section below.

## Training Procedure

For predicting prolonged ICU stay, static feature selection was performed by searching for pairs of correlated variables, calculating their mutual information scores, and discarding the variable with a lower score so that remaining variables had high information scores and low intervariable correlation. This process removed 15 variables from 100 candidates. XGBoost was then applied to five different subsets of the remaining 85 variables to find those with the strongest association with prolonged ICU stay, returning a final list of 17 static variables identified in any of the five searches; the features used to derive these 17 variables, were used in the final prediction model. We repeated this process to select features for predicting hospital mortality, but found that predictive performance was decreased substantially. Instead, we fit a decision tree regressor on the static features to quantify feature importance and tested model performance when excluding variables below feature importance thresholds of .01, .001, .0001, and found that optimal performance was obtained when excluding features with importance < .001; ten features (ASA class 1, unknown admission priority, unknown preoperative station, three primary services (Ophthalmology, Pediatric Surgery, and Plastic and Reconstructive Surgery) and four payers (Centers for Medicare & Medicaid, other payer, self-pay, and Workers’ Compensation) were thus excluded. Time series features, structured as hundreds of repeated values for each patient, are not amenable to either of the above feature selection methods. Instead, we performed leave-one-out testing for each time series feature and its missingness indicator by temporarily omitting a feature, recalculating performance, and permanently omitting that feature if performance was unchanged or improved. For predicting both prolonged ICU stay and hospital mortality, this process removed intraoperative FiO2 and its missingness indicator; all other intraoperative features remained.

All preoperative and intraoperative data were converted into tensors with fixed, uniform dimensions. Class weights for hospital mortality and prolonged ICU stay were calculated as inverted prevalence, so that rare events (e.g., hospital mortality) would have greater weight in model training. From the LightningModule from PyTorch Lightning (https://pytorch-lightning.readthedocs.io/en/latest/common/lightning_module.html) we used the rectified linear unit function on linear transformations of static binary and continuous variables and categorical embedding of CPT code and surgeon labels. Linear transformations of intraoperative variables were handled using a recurrent neural network and gated return units, as previously described.6 We used an Adam optimization algorithm that performs well for sparse or noisy gradients. To identify optimal hyperparameters, we performed a grid search by training and validating the model with every combination of sets of learning rates (1e-3 to 2e-5 by increments of 2e-5) batch sizes (32, 64, 128, 256), dropout rates (0.1, 0.2, 0.3, 0.4), and hidden dimensions (64, 74, 84, 94, 104); this process identified separate sets of hyperparameters for predicting hospital mortality and another optimal set for predicting prolonged ICU stay.

### Preprocessing

In model training, missing values were replaced with a potentially informative “missing” indicator; in reporting results from the validation cohort, missing values were imputed with medians, as previously described. To process intraoperative data, each time series for each variable in each index surgery was resampled to a list of timestamps representing each minute between surgery start and end times. Linear interpolation was used to impute missing values along a linear path between non-missing values before and after the missing value. Each intraoperative and static preoperative variable was also labeled as missing or not missing from the original data. Binary and continuous preoperative variables were standardized to have a mean of 0 and standard deviation of 1 (i.e., z-score normalization). Categorical embedding was performed on procedural codes and primary surgeon categories. For cases in which there were multiple primary surgeons, the primary surgeon affiliated with the primary service was given precedence. Labels for all procedure codes and surgeons that appeared 5 times or fewer in the training data were converted to single “other procedure” or “other surgeon” categories. 

### Speeds, Sizes, Times

The model was trained in approximately 2 days when using four GPUs.
 
# Evaluation

## Testing Data, Factors & Metrics

### Testing Data

The model was tested on 14,318 patients who had 15,673 admissions that included an inpatient operation at 3 hospitals within a single healthcare system.

Patient characteristics in the testing cohort are summarized here:
| Characteristic | Overtriage N=237 No (%) | Overtriage Controls N=1,021 No (%) | Undertriage N=1,029 No (%) | Undertriage Controls N=2,498 No (%) |
| -------------- | ------------------------ | --------------------------------- | --------------------------- | --------------------------------- |
| **Demographics** | | | | |
| Female | 91 (38.4) | 650 (63.7) | 499 (48.5) | 1025 (41.0) |
| Male | 146 (61.6) | 371 (36.3) | 530 (51.5) | 1473 (59.0) |
| Age (years), median [IQR] | 44.0 [32.0-55.0] | 44.0 [32.0-59.0] | 60.0 [49.0-71.0] | 56.0 [41.0-66.0] |
| **Race** | | | | |
| American Indian or Alaska Native | 0 (0.0) | 0 (0.0) | 2 (0.2) | 2 (0.1) |
| Asian | 1 (0.4) | 13 (1.3) | 7 (0.7) | 11 (0.4) |
| Black or African American | 74 (31.2) | 349 (34.2) | 402 (39.1) | 888 (35.5) |
| Native Hawaiian or Other Pacific Islander | 0 (0.0) | 0 (0.0) | 0 (0.0) | 5 (0.2) |
| Other or Multiracial | 17 (7.2) | 72 (7.1) | 47 (4.6) | 110 (4.4) |
| White | 143 (60.3) | 583 (57.1) | 565 (54.9) | 1475 (59.0) |
| Unknown | 2 (0.8) | 4 (0.4) | 6 (0.6) | 7 (0.3) |
| **Area Deprivation Index** | | | | |
| State rank, median [IQR] | 7.0 [5.0-9.0] | 7.0 [5.0-9.0] | 8.0 [5.0-10.0] | 8.0 [5.0-10.0] |
| National rank, median [IQR] | 69.0 [47.0-80.0] | 68.0 [47.0-85.0] | 70.0 [52.0-92.0] | 70.0 [52.0-92.0] |
| **Illness severity** | | | | |
| Admission SOFA score, median [IQR] | 2.0 [0.0-3.0] | 1.0 [0.0-3.0] | 3.0 [1.0-4.0] | 2.0 [1.0-3.0] |
| Charlson comorbidity index, median [IQR] | 0.0 [0.0-0.0] | 0.0 [0.0-1.0] | 2.0 [1.0-5.0] | 1.0 [0.0-2.0] |
| Admitted from emergency department | 139 (58.6) | 398 (39.0) | 623 (60.5) | 1611 (64.5) |
| Had preoperative red cell transfusion | 0 (0.0) | 5 (0.5) | 55 (5.3) | 59 (2.4) |
| **Admission priority status** | | | | |
| Elective | 94 (39.7) | 446 (43.7) | 332 (32.3) | 810 (32.4) |
| Urgent | 3 (1.3) | 88 (8.6) | 68 (6.6) | 79 (3.2) |
| Emergent | 139 (58.6) | 487 (47.7) | 629 (61.1) | 1608 (64.4) |
| Unknown | 1 (0.4) | 0 (0.0) | 0 (0.0) | 0 (0.0) |

IQR: interquartile range; SOFA: sequential organ failure assessment. Overtriage controls were ward admissions with risk profiles similar to those of overtriaged intensive care unit admissions. Undertriage controls were ICU admissions with risk profiles similar to those of undertriaged ward admissions.

Patient outcomes in the testing cohort are summarized here:
| Outcomes | Overtriage N=237 No (%) | Overtriage Controls N=1,021 No (%) | Undertriage N=1,029 No (%) | Undertriage Controls N=2,498 No (%) |
| --- | --- | --- | --- | --- |
| Resource use | | | | |
| ICU admission for ≥48 hours | 31 (13.1) | 14 (1.4) | 57 (5.5) | 1022 (40.9) |
| ICU length of stay (days), median [IQR] | 0.0 [0.0-0.9] | 0.0 [0.0-0.0] | 0.0 [0.0-0.0] | 1.2 [0.0-3.9] |
| Mechanical ventilation for ≥48 hours | 1 (0.4) | 14 (1.4) | 19 (1.8) | 41 (1.6) |
| Postoperative vasopressors within 2h of surgery | 0 (0.0) | 0 (0.0) | 1 (0.1) | 15 (0.6) |
| Second surgery during admission | 44 (18.6) | 60 (5.9) | 115 (11.2) | 376 (15.1) |
| Hours between surgeries, median [IQR] | 67 [46-104] | 71 [32-128] | 92 [49-153] | 72 [41-141] |
| Had emergent second surgery during admission | 5 (2.1) | 18 (1.8) | 25 (2.4) | 77 (3.1) |
| Had postoperative red cell transfusion | 3 (1.3) | 23 (2.3) | 117 (11.4) | 221 (8.8) |
| Red cell transfusion during admission | 3 (1.3) | 35 (3.4) | 182 (17.7) | 300 (12.0) |
| Hospital length of stay (days), median [IQR] | 3.1 [2.0-5.9] | 2.5 [1.9-4.6] | 6.4 [3.4-12.4] | 5.4 [2.6-10.4] |
| **Complications** | | | | |
| Hospital mortality | 0 (0.0) | 0 (0.0) | 17 (1.7) | 17 (0.7) |
| Discharge to hospice | 0 (0.0) | 2 (0.2) | 28 (2.7) | 17 (0.7) |
| Cardiac arrest | 0 (0.0) | 3 (0.3) | 14 (1.4) | 13 (0.5) |
| **Acute kidney injury** | | | | |
| Rapid reversal | 7 (3.0) | 54 (5.3) | 122 (11.9) | 250 (10.0) |
| Persistent, with renal recovery | 2 (0.8) | 11 (1.1) | 62 (6.0) | 95 (3.8) |
| Persistent, without renal recovery | 3 (1.3) | 16 (1.6) | 53 (5.2) | 70 (2.8) |
| **Charges and costs** | | | | |
| Professional service charges, $K, median [IQR] | 10.4 [7.3-16.8] | 8.4 [6.1-11.9] | 12.1 [8.2-19.7] | 14.6 [9.0-27.4] |
| Charges for hospital admission, $K, median [IQR] | 92.8 [62.7-128.0] | 63.6 [37.1-92.2] | 111.0 [71.8-167.5] | 114.3 [75.7-178.4] |
| Costs for hospital admission, $K, median [IQR] | 15.9 [9.8-22.3] | 10.7 [7.0-17.6] | 21.8 [13.3-34.9] | 21.9 [13.1-36.3] |
| Value of care, median [IQR] | 0.2 [0.1-0.3] | 1.5 [0.9-2.2] | 0.8 [0.5-1.3] | 1.2 [0.7-2.0] |

IQR: interquartile range, ICU: intensive care unit. we calculated value of care as inverted observed-to-expected in-hospital mortality and prolonged ICU stay ratios divided by median total costs and multiplied by a constant to set value of care for the entire study population to 1, as adapted from Yount, Jones, and colleagues (Yount KW, Turrentine FE, Lau CL, Jones RS. Putting the value framework to work in surgery. J Am Coll Surg. 2015 Apr;220(4):596-604.). 

### Factors

Two foreseeable characteristics that will influence how the model behaves include outcome incidence and EHR data availability.

### Metrics

Model discrimination was evaluated on the holdout test cohort by calculating area under the receiver operating characteristic curve (AUROC), area under the precision-recall curve (AUPRC), and by calculating Youden’s index to derive an optimal classification threshold for evaluating sensitivity, specificity, positive predictive value, negative predictive value, accuracy and F1 score. Calibration was assessed by calculating Brier scores. Ninety-five percent confidence intervals were computed for each metric using bootstrapping with 1,000 iterations. 
| Metric        | Prolonged ICU Stay          | In-hospital Mortality     |
| ------------- | --------------------------- | ------------------------- |
| Prevalence    | 9.6%                        | 2.1%                      |
| AUROC         | 0.87 (0.86-0.88)            | 0.91 (0.89-0.92)          |
| AUPRC         | 0.43 (0.40-0.45)            | 0.26 (0.21-0.30)          |
| Sensitivity   | 0.79 (0.78-0.84)            | 0.84 (0.77-0.91)          |
| Specificity   | 0.79 (0.75-0.81)            | 0.83 (0.72-0.88)          |
| PPV           | 0.29 (0.25-0.31)            | 0.09 (0.06-0.12)          |
| NPV           | 0.97 (0.97-0.98)            | 1.00 (0.99-1.00)          |
| Accuracy      | 0.79 (0.76-0.81)            | 0.83 (0.72-0.88)          |
| F1 score      | 0.42 (0.38-0.44)            | 0.17 (0.12-0.21)          |
| Brier score   | 0.10 (0.10-0.11)            | 0.08 (0.08-0.09)          |

## Results 

Overtriage to ICUs versus Risk-matched Ward Admissions:  

Compared with controls, overtriaged admissions had a greater incidence of prolonged (48 hours or greater) ICU stay (13.1 vs. 1.4%, P</=.001) and having a second surgery during admission (18.6 vs. 5.9%, P</=.001), though the incidence of emergent second surgery was similar between cohorts (2.1 vs. 1.8%, P=.97), as was the interval between index and second surgeries (67 [46-104] and 71 [32-128] hours, P=.97). Overtriage and control cohorts had similar incidence of postoperative red cell transfusion, acute kidney injury, cardiac arrest, and discharge to hospice, and there were no mortalities in either cohort (all P>.05). The overtriage cohort had greater total costs ($15.9K [$9.8K-$22.3K] vs. $10.7K [$7.0K-$17.6K], all P</=.001). For calculating value of care for the overtriage and control ward admission cohorts, observed mortality was imputed as 0.01% symmetrically to obtain a real number when calculating observed-to-expected mortality ratios as there were no observed mortalities in either cohort. Value was lower in the overtriage cohort (0.2 [0.1-0.3] vs. 1.5 [0.9-2.2], P</=.001.

Undertriage to General Wards versus Risk-matched ICU Admissions:

Approximately 9% (N=92/1,029) of all undertriaged admissions had subsequent ICU admission occurring at median 54 hours [interquartile range 18-145 hours] after surgery. Compared with controls, undertriaged admissions had a lower incidence of prolonged ICU stay (5.5 vs. 40.9%, P</=.001) but longer hospital length of stay (6.4 [3.4-12.4] vs. 5.4 [2.6-10.4] days, P</=.001). The undertriage cohort had a lower incidence of second surgery during admission (11.2 vs. 15.1%, P</=.001), though the incidence of emergent second surgery was similar between cohorts (2.4% and 3.1%, P=.46), and the incidence of postoperative red cell transfusion was greater in the undertriage cohort (11.4 vs. 8.8%, P=.05). The undertriage cohort had greater incidence of hospital mortality (1.7 vs. 0.7%, P=.03), discharge to hospice (2.7 vs. 0.7%, P</=.001), cardiac arrest (1.4 vs. 0.5%, P=.04), and persistent (lasting 72 hours or more) acute kidney injury with recovery of baseline renal function before discharge (6.0 vs. 3.8%, P=.01) and without renal recovery before discharge (5.2 vs. 2.8%, P=.002). Undertriage and control cohorts had similar total costs ($21.8K [$13.3K-34.9K] vs. $21.9K [$13.1K-36.3K], P=.97). Value of care was lower in the undertriage cohort (0.8 [0.5-1.3] vs. 1.2 [0.7-2.0], P</=.001).

Model Performance:

Prolonged ICU stay predictions had AUROC 0.920 (95% CI 0.916-0.923); hospital mortality predictions had AUROC 0.925 (0.909-0.940) (Table 5). AUPRC was higher for prolonged ICU stay predictions than for hospital mortality predictions, which was poor (0.755 (0.734-0.770) vs. 0.227 (0.175-0.304)), consistent with greater prevalence of prolonged ICU stay (19.3 vs. 1.0%).

# Model Examination

According to SHAP values, the top ten most important features for predicting prolonged ICU stay were intraoperative red cell transfusions, intraoperative vital signs, primary surgical service, and presence in an ICU prior to surgery; these features had clinically intuitive associations with increasing or decreasing risk for prolonged ICU stay when applied to an individual patient and the aggregate population. The top ten most important features for predicting hospital mortality were primary surgeon, comorbidities, admission SOFA scores, intraoperative vital signs, and elective surgery; these features had clinically intuitive associations with increasing or decreasing risk for hospital mortality when applied to an individual patient and the aggregate population.

# Environmental Impact

Carbon emissions can be estimated using the [Machine Learning Impact calculator](https://mlco2.github.io/impact#compute) presented in [Lacoste et al. (2019)](https://arxiv.org/abs/1910.09700).

- **Hardware Type:** GPUs
- **Hours used:** 46
- **Cloud Provider:** Private Infrastructure
- **Carbon Emitted:** 4.97 kg (this is similar to driving an average-sized, gasoline-powered car for about 20 miles)

# Technical Specifications

## Model Architecture and Objective

The model comprises fully supervised neural networks consisting of two main preoperative and intraoperative layers, each containing a preprocessing and feature extraction core and a data analytics core. In the analytics core, a multi-task model simultaneously predicts in-hospital mortality and prolonged ICU stay using one sub-model for preoperative features and one for intraoperative features. In the preoperative sub-model, individual nominal feature representations are derived from a learned, multidimensional per-feature embedding lookup table, concatenated, and passed through a fully connected layer. Representations of all numerical preoperative variables are obtained by passing input features through a fully connected layer. The intraoperative sub-model used a bidirectional recurrent neural network (RNN) with gated recurrent units. Intraoperative time series are passed through the RNN once in chronological order and once in reverse order. Time step representations are generated by concatenating RNN hidden states from the forward and backward passes. The preoperative sub-model runs end-to-end with the intraoperative sub-model to generate predictions at the end of the operation, when triage decisions are finalized. Continuous, nominal, and time series representations are concatenated and the result is passed through a fully connected layer and then through two branches for in-hospital mortality and prolonged ICU stay, each with one outcome-specific fully connected layer followed by a sigmoid activation function to produce per-outcome prediction scores, representing the probability of developing in-hospital mortality or prolonged ICU stay.

### Hardware

Trained with GPUs

### Software

pytorch

# Citation

**APA:**

Loftus, T. J., Ruppert, M. M., Shickel, B., Ozrazgat-Baslanti, T., Balch, J. A., Hu, D., Javed, A., Madbak, F., Skarupa, D. J., Guirgis, F., Efron, P. A., Tighe, P. J., Hogan, W. R., Rashidi, P., Upchurch, G. R., Jr, Bihorac, A. (2023). Overtriage, Undertriage, and Value of Care after Major Surgery: An Automated, Explainable Deep Learning-Enabled Classification System. Journal of the American College of Surgeons, 236(2), 279–291. https://doi.org/10.1097/XCS.0000000000000471

**BibTeX:**

@article{RN12285,
   author = {Loftus, T. J. and Ruppert, M. M. and Shickel, B. and Ozrazgat-Baslanti, T. and Balch, J. A. and Hu, D. and Javed, A. and Madbak, F. and Skarupa, D. J. and Guirgis, F. and Efron, P. A. and Tighe, P. J. and Hogan, W. R. and Rashidi, P. and Upchurch, G. R., Jr. and Bihorac, A.},
   title = {Overtriage, Undertriage, and Value of Care after Major Surgery: An Automated, Explainable Deep Learning-Enabled Classification System},
   journal = {J Am Coll Surg},
   volume = {236},
   number = {2},
   pages = {279-291},
   ISSN = {1879-1190 (Electronic)
1072-7515 (Print)
1072-7515 (Linking)},
   DOI = {10.1097/XCS.0000000000000471},
   url = {https://www.ncbi.nlm.nih.gov/pubmed/36648256},
   year = {2023},
   type = {Journal Article}
}

# More Information 

APARI version 2.0 will be made available upon publication.

# Model Card Authors 

Matthew M Ruppert, Die Hu, Benjamin Shickel, Tyler J Loftus

# Model Card Contact

tyler.loftus@surgery.ufl.edu

# How to Get Started with the Model

Use the code below to load a pretrained APARI checkpoint and generate predictions.

<details>
<summary> Click to expand </summary>


```python
import torch
from Python.Model_Toolbox.Python.Pytorch.Model_and_Layers import Model
from Python.Model_Toolbox.Python.Pytorch.Training import load_data, get_predictions

# 1. Select model checkpoint (inside Best_Model folder of this repo)
ckpt_path = "Best_Model/best_mortality_model.ckpt"  # or Best_Model/best_prolonged_icu_model.ckpt
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 2. Load model
checkpoint = torch.load(ckpt_path, map_location=device, weights_only=False)
config = checkpoint['hyper_parameters']['config'].copy()
config['dataset']['h5_file'] = "your_dataset.h5"  # path to your preprocessed dataset in H5 format

model = Model.load_from_checkpoint(ckpt_path, config=config, map_location=device)
model.to(device)
model.eval()

# 3. Load dataset (train/val/test splits, here we just use test)
_, _, test_data = load_data(config=config)

# 4. Generate predictions
predictions_df = get_predictions(model=model, data=test_data, config=config)

# 5. Inspect results
print(predictions_df.head())  #output a DataFrame containing subject IDs, true labels, and predicted probabilities for the chosen outcome.


