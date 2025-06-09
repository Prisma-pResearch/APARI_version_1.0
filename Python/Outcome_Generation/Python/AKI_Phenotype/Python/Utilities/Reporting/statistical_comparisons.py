# -*- coding: utf-8 -*-
"""
Statistical Comparison Module.

Created on Mon Jan 31 17:03:31 2022

@author: tloftus/ruppert20
"""
import pandas as pd
import scipy as sp
from scipy.stats import shapiro, f_oneway
# from statsmodels.stats.multicomp import pairwise_tukeyhsd
from tqdm import tqdm
from itertools import combinations
from ..PreProcessing.data_format_and_manipulation import check_format_series
from ..PreProcessing.standardization_functions import _get_column_type
from typing import Union
from collections import namedtuple


def chi2_crosstab(input_df: pd.DataFrame, column_list: list, level_column: str) -> pd.DataFrame:
    """
    Perfrom chi-squared crosstab analyses and generate a dataframe containing results.

    Parameters
    ----------
    input_df: pd.DataFrame
        pandas dataframe that is being used for the statistical analysis
    column_list: list
        customizable list of columns from the input_df
    level_column: str
        the name of the column in the input_df that contains group assignments (e.g., sex)

    Actions
    -------
    1. calculate and save n(%) representing the count and % of total that the condition represented
        by the column name in column_list is present
    2. calculate and save Pearsons chi-square test for independence test statistic, p-value, and degrees of freedom

    Returns
    -------
    output_df: pd.DataFrame
        pandas dataframe containing results of the analysis

    """
    # instatiate a dataframe and name its columns
    output_df = pd.DataFrame(columns=['column', 'chi_square_statistic', 'p_value', 'degrees_of_freedom'])

    # iterate through columns that are in the customizable column_list
    for column in column_list:
        # instantiate a temporary dataframe that calls the crosstab function on the input dataframe columns and column levels
        # use .T to transpose the dataframe
        temp = pd.crosstab(input_df[level_column], input_df[column]).T
        # instatiate a new row that saves n(%) for each level_column level with 1 decimal place for %
        for col in temp.columns:
            temp.loc[1, col] = f'{temp.loc[1, col]} ({((temp.loc[1, col] / temp[col].sum())*100): .1f})'
        # instatiate a second temporary dataframe that calls the chi2_contingency function on the crosstab
        temp_2 = sp.stats.chi2_contingency(pd.crosstab(input_df[level_column], input_df[column]))

        # add columns to the temp dataframe
        temp['chi_square_statistic'] = None
        temp['p_value'] = None
        temp['degrees_of_freedom'] = None
        temp['column'] = column

        # fill columns in the temp dataframe with positional values from the second temp dataframe
        temp.loc[1, 'chi_square_statistic'] = temp_2[0]
        temp.loc[1, 'p_value'] = temp_2[1]
        temp.loc[1, 'degrees_of_freedom'] = temp_2[2]

        # concatenate the output dataframe with the temp dataframe
        output_df = pd.concat([output_df, temp], axis=0, sort=False)

    # return the final output dataframe, keeping only the row with index =1 (condition for the column list is present)
    return output_df.drop(index=0)


def fisher_crosstab(input_df: pd.DataFrame, column_basis_dict: dict, level_column: str, abbrev_delimeter: str = 'XXXYYYXXX', raw_output: bool = False) -> pd.DataFrame:
    """
    Perfrom Fishers Exact test crosstab analyses and generate a dataframe containing results.

    Parameters
    ----------
    input_df: pd.DataFrame
        pandas dataframe that is being used for the statistical analysis
    column_basis_dict: dict
        customizable list of columns from the input_df with the base class e.g. {'sex': 'MALE'}
    level_column: str
        the name of the column in the input_df that contains group assignments (e.g., sex)
    abbrev_delimeter: str, optional
        Delimeter in column names. The default is 'XXXYYYXXX' that will not modify the column names.
    raw_output: bool, optinal
        Whether the output should be as is. The default is False, which will round the fisher exact test result to three significant figures.

    Actions
    -------
    1. calculate and save n(%) representing the count and % of total that the condition represented
        by the column name in column_list is present
    2. calculate and save the Fishers Exact test statistic and p-value

    Returns
    -------
    output_df: pd.DataFrame
        pandas dataframe containing results of the analysis

    """
    # define unique levels
    levels: list = input_df[level_column].dropna().unique().tolist()

    # define pairs
    pairs: list = list(combinations(levels, 2))

    # instatiate a dataframe and name its columns
    output_df = pd.DataFrame(columns=levels
                             + [f'{x[0]} vs. {x[1]}' for x in pairs])

    # iterate through columns that are in the customizable column_list
    for column, base_class in column_basis_dict.items():
        assert input_df[column].nunique() == 2, f'There must be two classes in each column_basis_dict column, however, the following levels were found in column: {column} levels: {input_df[column]}'
        # pass
        for pair in pairs:
            # pass
            temp_df = input_df.loc[input_df[level_column].isin(list(pair)), [column, level_column]]

            temp_df = pd.crosstab(temp_df[level_column], temp_df[column])

            fisher_result = sp.stats.fisher_exact(temp_df)

            output_df.loc[f'{column} ({base_class})',
                          f'{pair[0].split(abbrev_delimeter)[0]} vs. {pair[1].split(abbrev_delimeter)[0]}'] = f'{fisher_result[1] if raw_output else 0 if fisher_result[1] < 0.001 else fisher_result[1]:.3f}'\
                .replace('1.000', '>.99').replace('0.000', '<.001')

        for level in levels:
            count = sum((input_df[level_column] == level) & (input_df[column] == base_class))
            denominator = sum(input_df[level_column] == level)

            output_df.loc[f'{column} ({base_class})', level] = f'{count} ({count/denominator:.1%})'

    return output_df.reset_index().rename(columns={'index': 'column'})


def format_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    """Format the kruskal_wallace() dataframe."""
    df['value'] = None

    for i, row in df.iterrows():
        df.iloc[i, df.columns.get_loc('value')] = f"{(row['median']):.1f} [{row['q1']:.1f}-{(row['q3']):.1f}]"

    df = df.drop(columns=['median', 'q1', 'q3'])

    return df.T


def cleanup_df(df: pd.DataFrame, index_col: str) -> pd.DataFrame:
    """Reshape the kruskal_wallace() dataframe."""
    df = df[df.level_1 != 'column']

    new_column_names = df[df.level_1 == index_col].iloc[0, :].tolist()

    new_column_names[0] = 'column'
    new_column_names[1] = 'temp'

    df = df[df.level_1 != index_col]

    df.columns = new_column_names

    return df.drop(columns=['temp'])


def kruskal_wallace(input_df: pd.DataFrame, column_list: list, level_column: str, abbrev_delimeter: str = None) -> pd.DataFrame:
    """
    Perfrom Fishers Exact test crosstab analyses and generate a dataframe containing results.

    Parameters
    ----------
    input_df: pd.DataFrame
        pandas dataframe that is being used for the statistical analysis
    column_list: list
        customizable list of columns from the input_df
    level_column: str
        the name of the column in the input_df that contains group assignments (e.g., sex)

    Actions
    -------
    1. calculate and save mean, standard deviation, median, 25%, and 75% for each group
    2. calculate and save the test for independence across all rows in the row_series,
        as well as the test statistic and degrees of freedom

    Returns
    -------
    output_df: pd.DataFrame
        pandas dataframe containing results of the analysis

    """
    # instatiate a statistical comparison dataframe and name its columns
    statistic_df = pd.DataFrame(columns=['column', 'comparison', 'p_value', 'kruskal_test_statistic'])

    # instantiate a summary stats dataframe and name its columns
    summary_df = pd.DataFrame(columns=['column', 'levels', 'median', 'q1', 'q3'])

    # define unique levels
    levels: list = input_df[level_column].dropna().unique().tolist()

    # define pairs
    pairs: list = list(combinations(levels, 2))

    print(pairs)

    # instantiate a final output dataframe with formatting of outputs
    output_df = pd.DataFrame(columns=['column']
                             + levels
                             + [f'{x[0]} vs. {x[1]}' for x in pairs if isinstance(abbrev_delimeter, str)])

    # group the dataframe by the level column labels
    grouped_df = input_df.groupby(level_column).describe()

    # iterate through columns that are in the customizable column_list and calculate stats for column variables
    for column in column_list:
        print(column)
        temp_sum = grouped_df.loc[:, (column, ['50%', '25%', '75%']), ]\
            .rename(columns={'50%': 'median',
                             '25%': 'q1',
                             '75%': 'q3'})\
            .reset_index()\
            .rename(columns={'': 'levels'})
        temp_sum.columns = temp_sum.columns.get_level_values(1)

        temp_sum['column'] = column

        summary_df = pd.concat([summary_df,
                                temp_sum],
                               axis=0, sort=False)

        # use the pairs package so that this function will work for any number of levels
        for pair in pairs:

            # instantiate a second temp df that performs the kruskal wallis test
            temp = sp.stats.kruskal(input_df[column][((input_df[level_column] == pair[0]) & input_df[column].notnull())],
                                    input_df[column][((input_df[level_column] == pair[1]) & input_df[column].notnull())])

            statistic_df = pd.concat([statistic_df, pd.DataFrame({'column': [column],
                                                                  'comparison': [f'{pair[0]} vs. {pair[1]}' if isinstance(abbrev_delimeter, str) else f'{pair[0]} vs. {pair[1]}'],
                                                                  'kruskal_test_statistic': [temp[0]],
                                                                  'p_value': [f'{0 if temp[1] < 0.001 else temp[1]:.3f}'.replace('1.000', '>.99').replace('0.000', '<.001')]})], axis=0, sort=False)

    # call the custum cleanup_df function on the merged summary and statistic dataframes
    output_df = cleanup_df(df=summary_df.groupby('column').apply(format_summary_df).reset_index(),
                           index_col='levels')\
        .merge(cleanup_df(df=statistic_df.groupby('column')
                          .apply(lambda df: df.drop(columns=['kruskal_test_statistic']).T).reset_index(),
                          index_col='comparison'),
               how='left',
               on='column')

    # return the final output dataframe, keeping only the row with index =1 (condition for the column list is present)
    return output_df


def raw_kruskal_wallace(input_df: pd.DataFrame, column_list: list, level_column: str, abbrev_delimeter: str = None) -> pd.DataFrame:
    """
    Perfrom Fishers Exact test crosstab analyses and generate a dataframe containing results.

    Parameters
    ----------
    input_df: pd.DataFrame
        pandas dataframe that is being used for the statistical analysis
    column_list: list
        customizable list of columns from the input_df
    level_column: str
        the name of the column in the input_df that contains group assignments (e.g., sex)

    Actions
    -------
    1. calculate and save mean, standard deviation, median, 25%, and 75% for each group
    2. calculate and save the test for independence across all rows in the row_series,
        as well as the test statistic and degrees of freedom

    Returns
    -------
    output_df: pd.DataFrame
        pandas dataframe containing results of the analysis

    """
    # instatiate a statistical comparison dataframe and name its columns
    statistic_df = pd.DataFrame(columns=['column', 'comparison', 'p_value', 'kruskal_test_statistic'])

    # instantiate a summary stats dataframe and name its columns
    summary_df = pd.DataFrame(columns=['column', 'levels', 'median', 'q1', 'q3'])

    # define unique levels
    levels: list = input_df[level_column].dropna().unique().tolist()

    # define pairs
    pairs: list = list(combinations(levels, 2))

    print(pairs)

    # instantiate a final output dataframe with formatting of outputs
    output_df = pd.DataFrame(columns=['column']
                             + levels
                             + [f'{x[0]} vs. {x[1]}' for x in pairs if isinstance(abbrev_delimeter, str)])

    # group the dataframe by the level column labels
    grouped_df = input_df.groupby(level_column).describe()

    # iterate through columns that are in the customizable column_list and calculate stats for column variables
    for column in column_list:
        print(column)
        temp_sum = grouped_df.loc[:, (column, ['50%', '25%', '75%']), ]\
            .rename(columns={'50%': 'median',
                             '25%': 'q1',
                             '75%': 'q3'})\
            .reset_index()\
            .rename(columns={'': 'levels'})
        temp_sum.columns = temp_sum.columns.get_level_values(1)

        temp_sum['column'] = column

        summary_df = pd.concat([summary_df,
                                temp_sum],
                               axis=0, sort=False)

        # use the pairs package so that this function will work for any number of levels
        for pair in pairs:

            # instantiate a second temp df that performs the kruskal wallis test
            temp = sp.stats.kruskal(input_df[column][((input_df[level_column] == pair[0]) & input_df[column].notnull())],
                                    input_df[column][((input_df[level_column] == pair[1]) & input_df[column].notnull())])

            statistic_df = pd.concat([statistic_df, pd.DataFrame({'column': [column],
                                                                  'comparison': [f'{pair[0]} vs. {pair[1]}' if isinstance(abbrev_delimeter, str) else f'{pair[0]} vs. {pair[1]}'],
                                                                  'kruskal_test_statistic': [temp[0]],
                                                                  'p_value': [temp[1]]})], axis=0, sort=False)

    # call the custum cleanup_df function on the merged summary and statistic dataframes
    output_df = cleanup_df(df=summary_df.groupby('column').apply(format_summary_df).reset_index(),
                           index_col='levels')\
        .merge(cleanup_df(df=statistic_df.groupby('column')
                          .apply(lambda df: df.drop(columns=['kruskal_test_statistic']).T).reset_index(),
                          index_col='comparison'),
               how='left',
               on='column')

    # return the final output dataframe, keeping only the row with index =1 (condition for the column list is present)
    return output_df


def test_normality(input_df: pd.DataFrame, column_list: list) -> pd.DataFrame:
    """
    Perform the Shapiro Wilk test on a list of continuous variables.

    Parameters
    ----------
    input_df: pd.DataFrame
        the dataframe contining the variables and data for which normality is
        being tested
    columns_list : list
        a list of column names for continuous variables

    Returns
    -------
        pd.DataFrame

    """
    col_list: list = []
    sw_stats: list = []
    sw_p_value: list = []

    input_df.dropna(axis=0, how='any', subset=column_list, inplace=True)

    for column in column_list:
        stat, p = shapiro(input_df[column])
        col_list.append(column)
        sw_stats.append(stat)
        sw_p_value.append(p)

    return pd.DataFrame({'variable': col_list, 'sw_stat': sw_stats, 'sw_p': sw_p_value})


StatItem = namedtuple('StatItem', 'test statistic pvalue')


def _run_categorical_comparison(series: pd.Series, base_level: any = None) -> StatItem:
    if base_level is not None:
        series = (series == base_level).astype(int)

    # make contingency table
    cont_table: pd.DataFrame = pd.crosstab(series.dropna().index, series.dropna(), margins=False)
    groups: list = series.index.unique().tolist()

    if cont_table.min().min() > 5:  # Chi Square Requires expected value of at least 5
        # run chi Square test
        chi2, p, dof, _ = sp.stats.chi2_contingency(cont_table.values)

        return StatItem('Chi-squared', chi2, p)

    else:
        # run fisher exact if two groups and two levels
        if cont_table.shape == (2, 2):
            fisher = sp.stats.fisher_exact(cont_table)

            return StatItem('Fisher’s exact', fisher[0], fisher[1])
        else:
            # run kruskall Wallis if three or more
            stat_info = sp.stats.kruskal(*tuple(series.loc[g] for g in groups), nan_policy='omit')

            return StatItem('Kruskal Wallis', stat_info.statistic, stat_info.pvalue)


def _run_numeric_comparison(series: pd.Series, iid: bool = None) -> StatItem:

    iid: bool = iid or (shapiro(series).pvalue >= 0.05)
    groups: list = series.index.unique().tolist()

    if iid:
        # run one way anova
        stat_info = f_oneway(*tuple(series.loc[g] for g in groups))
        test: str = 'One Way ANOVA'
    else:
        # run kruskal Wallis test
        stat_info = sp.stats.kruskal(*tuple(series.loc[g] for g in groups), nan_policy='omit')
        test: str = 'Kruskal Wallis'

    return StatItem(test, stat_info.statistic, stat_info.pvalue)


def summarize_groups(input_v: Union[dict, pd.DataFrame],
                     group_col: str = None,
                     config_dict: dict = {'sex': {'base_group': 'MALE'},
                                          'ethnicity': {'base_group': 'HISPANIC'},
                                          'race': {'standardization_dict': {'HISPANIC': 'WHITE',
                                                                            'WHITE HISPANIC': 'WHITE',
                                                                            'PATIENT REFUSED': 'OTHER/UNKNOWN',
                                                                            'UNKNOWN': 'OTHER/UNKNOWN',
                                                                            'OTHER': 'OTHER/UNKNOWN',
                                                                            'PACIFIC ISLANDER': 'OTHER/UNKNOWN',
                                                                            # 'ASIAN': 'OTHER/UNKNOWN',
                                                                            'MULTIRACIAL': 'OTHER/UNKNOWN',
                                                                            'AMERICAN INDIAN': 'OTHER/UNKNOWN'}},
                                          'age': {'unit': 'years',
                                                  'dtype': 'int',
                                                  'iid': False},
                                          'hosp_los': {'unit': 'days',
                                                       'dtype': 'float',
                                                       'iid': False},
                                          'icu_los': {'unit': 'days',
                                                      'dtype': 'float',
                                                      'iid': False}
                                          },
                     max_levels_to_display: int = 5,
                     other_unknown_level: str = 'other/unknown',
                     cols_to_ignore: list = []) -> pd.DataFrame:
    """
    Summarize groups and differences accros/between groups.

    Actions:
        1. Determine data type (if one is not provided)
        2. Summarize values and differences
            For Numeric (float, int) variables summarize based on whether the variable is assumed to be iid or not.
                if iid:
                    summarize via Mean (standard deviation)
                    check for differences via One Way ANOVA
                else:
                    summarize via Median (Q1 = Q3)
                    check for differences via kruskcall wallace

            For categorical values
                summarize via count percentage

                if expected value greater than 5:  # requires expeted value of at least five
                    check for differences using chi square
                else
                    if two levels and two groups:
                        check for differences using fischer exact
                    else:
                        check for differences using Kruskell wallace
    Parameters
    ----------
        input_v: Union[dict, pd.DataFrame],
        group_col: str = None,
        config_dict: dict = {'sex': {'base_group': 'MALE'},
                             'ethnicity': {'base_group': 'HISPANIC'},
                             'race': {'standardization_dict': {'HISPANIC': 'WHITE',
                                                               'WHITE HISPANIC': 'WHITE',
                                                               'PATIENT REFUSED': 'OTHER/UNKNOWN',
                                                               'UNKNOWN': 'OTHER/UNKNOWN',
                                                               'OTHER': 'OTHER/UNKNOWN',
                                                               'PACIFIC ISLANDER': 'OTHER/UNKNOWN',
                                                               # 'ASIAN': 'OTHER/UNKNOWN',
                                                               'MULTIRACIAL': 'OTHER/UNKNOWN',
                                                               'AMERICAN INDIAN': 'OTHER/UNKNOWN'}},
                             'age': {'unit': 'years',
                                     'dtype': 'int',
                                     'iid': False},
                             'hosp_los': {'unit': 'days',
                                          'dtype': 'float',
                                          'iid': False},
                             'icu_los': {'unit': 'days',
                                         'dtype': 'float',
                                         'iid': False}
                             },
        max_levels_to_display: int = 5,
        cols_to_ignore: list = []
    """
    assert isinstance(input_v, (dict, pd.DataFrame)), f'The input_v parameter must be a pandas dataframe or dictionary of pandas dataframes, however; it was found to be of type: {type(input_v)}'
    if isinstance(input_v, dict):
        group_col: str = group_col or 'group_col'
        df_l: list = []
        for g, df in input_v.items():
            assert isinstance(df, pd.DataFrame), f'The object for key: {g} must be a pandas dataframe, however; it was found to be of type {type(df)}'
            df[group_col] = g
            df_l.append(df)
        temp_df: pd.DataFrame = pd.concat(df_l, axis=0, ignore_index=True).set_index(group_col)
    else:
        assert group_col in input_v.columns, f'The group_col: {group_col} was not found in the input_v dataframe'
        temp_df: pd.DataFrame = input_v.copy(deep=True).set_index(group_col)

    groups: list = temp_df.index.unique().tolist()
    columns: list = temp_df.columns.difference(cols_to_ignore).tolist()

    out: pd.DataFrame = pd.DataFrame(columns=groups + ['test', 'test_statistic', 'p-value', 'summary_stat'])

    for g in groups:
        g_size: int = (temp_df.index == g).sum()
        out.loc['cohort_size', g] = f'{g_size:,.0f} ({g_size / temp_df.shape[0]:.1%})'

    for col in tqdm(columns, desc='Computing Comparisions'):
        confd: dict = config_dict.get(col, {})
        col_dtype: str = str(confd.get('dtype',
                                       _get_column_type(series=temp_df[col],
                                                        one_hot_threshold=max_levels_to_display))).lower()

        std_dict: dict = confd.get('standardization_dict', {})

        ot_unk: str = std_dict.get('other_unknown', other_unknown_level)

        series: pd.Series = check_format_series(ds=temp_df[col].copy(), desired_type=col_dtype)

        if col_dtype in ['str', 'sparse_int', 'cat_embedding', 'object', 'cat_one_hot']:
            series = series.apply(lambda x: std_dict.get(x, ot_unk))
        else:
            series.replace(std_dict, inplace=True)

        if col_dtype in ['int', 'float']:
            col_label: str = f"{col} ({confd.get('unit')})" if confd.get('unit') is not None else col

            stat_info: StatItem = _run_numeric_comparison(series=series.copy(), iid=confd.get('iid', None))

            out.loc[col_label, ['test', 'test_statistic', 'p-value']] = stat_info.test, stat_info.statistic, stat_info.pvalue

            # generate stats for each group
            for group in groups:
                if stat_info.test == 'One Way ANOVA':
                    mean, std = series.loc[group].describe().loc[['mean', 'std']]
                    out.loc[col_label, group] = f'{mean:.0f} ({std:.0f})' if col_dtype in ['int'] else f'{mean:.1f} ({std:.1f})'
                    out.loc[col_label, 'summary_stat'] = 'mean (standard deviation)'
                else:
                    med, q1, q3 = series.loc[group].describe().loc[['50%', '25%', '75%']]
                    out.loc[col_label, group] = f'{med:.0f} ({q1:.0f} - {q3:.0f})' if col_dtype in ['int'] else f'{med:.1f} ({q1:.1f} - {q3:.1f})'
                    out.loc[col_label, 'summary_stat'] = 'median (Q1 - Q3)'
        else:
            # filter for top n
            series: pd.Series = check_format_series(ds=series.copy(), desired_type='cat_top_n', top_n=max_levels_to_display, other_unknown_cat=ot_unk)

            stat_info: StatItem = _run_categorical_comparison(series=series, base_level=None)

            # save values
            out.loc[col, ['test', 'test_statistic', 'p-value']] = stat_info.test, stat_info.statistic, stat_info.pvalue

            base_grp: any = confd.get('base_group', None)

            for lev in ([base_grp] if base_grp is not None else series.unique().tolist()):
                for group in groups:
                    temp: pd.Series = (series.loc[group] == lev).astype(int)
                    count: int = (temp).sum().astype(int)
                    out.loc[f'{col} ({lev})', group] = f'{count:,.0f} ({count / temp.shape[0]:.1%})'
                    out.loc[f'{col} ({lev})', 'summary_stat'] = 'n (%)'
                # make comparison for level
                stat_info: StatItem = _run_categorical_comparison(series=series, base_level=lev)

                out.loc[f'{col} ({lev})', ['test', 'test_statistic', 'p-value']] = stat_info.test, stat_info.statistic, stat_info.pvalue
    return out
