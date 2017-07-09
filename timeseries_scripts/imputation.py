"""
Open Power System Data

Timeseries Datapackage

imputation.py : fill functions for imputation of missing data.

"""

from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)
logger.setLevel('INFO')


def find_nan(df, res_key, headers, patch=False):
    '''
    Search for missing values in a DataFrame and optionally apply further 
    functions on each column.

    Parameters
    ----------    
    df : pandas.DataFrame
        DataFrame to inspect and possibly patch
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    patch : bool, default=False
        If False, return unaltered DataFrame,
        if True, return patched DataFrame

    Returns
    ----------    
    patched: pandas.DataFrame
        original df or df with gaps patched and marker column appended
    nan_table: pandas.DataFrame
        Contains detailed information about missing data

    '''
    nan_table = pd.DataFrame()
    patched = pd.DataFrame()
    marker_col = pd.Series(np.nan, index=df.index)

    if df.empty:
        return patched, nan_table

    # Get the frequency/length of one period of df
    one_period = pd.Timedelta(res_key)
    for col_name, col in df.iteritems():
        col = col.to_frame()
        message = '| {:5.5} | {:6.6} | {:10.10} | {:10.10} | {:10.10} | '.format(
            res_key, *col_name[0:4])

        # make an empty list of NaN regions to use as default
        nan_idx = pd.MultiIndex.from_arrays([
            [0, 0, 0, 0],
            ['count', 'span', 'start_idx', 'till_idx']])
        nan_list = pd.DataFrame(index=nan_idx, columns=col.columns)

        # skip all columns from ENTSO-E Transparency as it takes too long
        # if  col_name[headers.index('source')] == 'ENTSO-E Transparency':
        #    if patched.empty:
        #        patched = col
        #    else:
        #        patched = patched.combine_first(col)
        #
        #    if nan_table.empty:
        #        nan_table = nan_list
        #    else:
        #        nan_table = nan_table.combine_first(nan_list)
        #    continue

        # skip this column if it has no entries at all.
        # This will also delete the column from the patched df
        if col.empty:
            continue

        # tag all occurences of NaN in the data with True
        # (but not before first or after last actual entry)
        col['tag'] = (
            (col.index >= col.first_valid_index()) &
            (col.index <= col.last_valid_index()) &
            col.isnull().transpose().as_matrix()
        ).transpose()

        # make another DF to hold info about each region
        nan_regs = pd.DataFrame()

        # first row of consecutive region is a True preceded by a False in tags
        nan_regs['start_idx'] = col.index[col['tag'] & ~
                                          col['tag'].shift(1).fillna(False)]

        # last row of consecutive region is a False preceded by a True
        nan_regs['till_idx'] = col.index[
            col['tag'] & ~
            col['tag'].shift(-1).fillna(False)]

        # if there are no NaNs, do nothing
        if not col['tag'].any():
            logger.info(message + 'nothing to patch in this column')
            col.drop('tag', axis=1, inplace=True)

        # else make a list of the NaN regions
        else:
            # how long is each region
            nan_regs['span'] = (
                nan_regs['till_idx'] - nan_regs['start_idx'] + one_period)
            nan_regs['count'] = (nan_regs['span'] / one_period)
            # sort the nan_regs DataFtame to put longest missing region on top
            nan_regs = nan_regs.sort_values(
                'count', ascending=False).reset_index(drop=True)

            col.drop('tag', axis=1, inplace=True)
            nan_list = nan_regs.stack().to_frame()
            nan_list.columns = col.columns

            if patch:
                col, marker_col = choose_fill_method(
                    message,
                    col, col_name, nan_regs, df, marker_col, one_period)

        if patched.empty:
            patched = col
        else:
            patched = patched.combine_first(col)

        if nan_table.empty:
            nan_table = nan_list
        else:
            nan_table = nan_table.combine_first(nan_list)

    # append the marker to the DataFrame
    marker_col = marker_col.to_frame()
    tuples = [('interpolated_values', '', '', '', '', '')]
    marker_col.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    patched = pd.concat([patched, marker_col], axis=1)

    # set the level names for the output
    nan_table.columns.names = headers
    patched.columns.names = headers

    return patched, nan_table


def choose_fill_method(
        message, col, col_name, nan_regs, df, marker_col, one_period):
    '''
    Choose the appropriate function for filling a region of missing values.

    Parameters
    ----------  
    col : pandas.DataFrame
        A column from frame as a separate DataFrame 
    col_name : tuple
        tuple of header levels of column to inspect
    nan_regs : pandas.DataFrame
        DataFrame with each row representing a region of missing data in col
    df : pandas.DataFrame
        DataFrame to patch with n rows
    marker_col : pandas.DataFrame
        An n*1 DataFrame specifying for each row which of the previously treated 
        columns have been patched
    one_period : pandas.Timedelta
        Time resolution of frame and col (15/30/60 minutes)

    Returns
    ----------  
    col : pandas.DataFrame
        An n*1 DataFrame containing col with nan_regs filled
        and another column for the marker
    marker_col: pandas.DataFrame
        Definition as under Parameters, but now appended with markers for col 

    '''
    for i, nan_region in nan_regs.iterrows():
        j = 0
        # Interpolate missing value spans up to 2 hours
        if nan_region['span'] <= timedelta(hours=2):
            col, marker_col = my_interpolate(
                i, j, nan_region, col, col_name, marker_col, nan_regs,
                one_period, message)
        # Guess missing value spans longer than one hour based on other tsos
        # (Only for German wind and solar generation data)
        elif col_name[1][:2] == 'DE' and col_name[2] == 'generation':
            j += 1

            # NOT IMPLEMENTED
            # col = impute(nan_region, col, col_name, nan_regs, df, one_period)

    return col, marker_col


def my_interpolate(
        i, j, nan_region, col, col_name, marker_col, nan_regs, one_period, message):
    '''
    Interpolate one missing value region in one column as described by 
    nan_region.

    The default pd.Series.interpolate() function does not work if
    interpolation is to be restricted to periods of a certain length.
    (A limit-argument can be specified, but it results in longer periods 
    of missing data to be filled parcially) 

    Parameters
    ----------
    i : int
        Counter for total number of regions of missing data
    j : int
        Counter for number regions of missing data not treated by by this
        function
    nan_region : pandas.Series
        Contains information on one region of missing data in col
        count: 
        span:
        start_idx:
        till_idx:
    See choose_fill_method() for info on other parameters.

    Returns
    ----------
    col : pandas.DataFrame
        The column with all nan_regs treated for periods shorter than 1:15.

    '''
    if i + 1 == len(nan_regs):
        treated = i + 1 - j
        logger.info(message + 'interpolated %s up-to-2-hour-span(s) of NaNs',
                    treated)

    to_fill = slice(nan_region['start_idx'] - one_period,
                    nan_region['till_idx'] + one_period)
    comment_now = slice(nan_region['start_idx'], nan_region['till_idx'])

    col.iloc[:, 0].loc[to_fill] = col.iloc[:, 0].loc[to_fill].interpolate()

    # Create a marker column to mark where data has been interpolated
    col_name_str = '_'.join(
        [level for level in col_name[0:3] if not level == ''])

    comment_before = marker_col.notnull()
    comment_again = comment_before.loc[comment_now]
    if comment_again.any():
        marker_col[comment_before & comment_again] = marker_col + \
            ' | ' + col_name_str
    else:
        marker_col.loc[comment_now] = col_name_str

    return col, marker_col


# Not implemented: For the generation timeseries, larger gaps are guessed
# by up-/down scaling the data from other balancing areas to fit the
# expected magnitude of the missing data.


def impute(nan_region, col, col_name, nan_regs, df, one_period):
    '''
    Impute missing value spans longer than one hour based on other TSOs.

    Parameters
    ----------
    nan_region : pandas.Series
        Contains information on one region of missing data in col
    col : pandas.DataFrame
        A column from df as a separate DataFrame 
    col_name : tuple
        tuple of header levels of column to inspect
    nan_regs : : pandas.DataFrame
        DataFrame with each row representing a region of missing data in col
    df : pandas.DataFrame
        DataFrame to patch
    one_period : pandas.Timedelta
        Time resolution of df and col (15/60 minutes)

    '''
    #logger.info('guessed %s entries after %s', row['count'], row['start_idx'])
    day_before = pd.DatetimeIndex(
        freq='15min',
        start=nan_region['start_idx'] - timedelta(hours=24),
        end=nan_region['start_idx'] - one_period)

    to_fill = pd.DatetimeIndex(
        freq='15min',
        start=nan_region['start_idx'],
        end=nan_region['till_idx'])

    # other_tsos = [c[1] for c in compact.drop(col_name, axis=1)
    #.loc[:,(col_name[0],slice(None),col_name[2])].columns.tolist()]
    other_tsos = [tso
                  for tso in ['DE-50Hertz', 'DE-Amprion', 'DE-TenneT', 'DE-TransnetBW']
                  if tso != col_name[1]]

    # select columns with data for same technology (wind/solar) but from other
    # TSOs
    similar = df.loc[:, (col_name[0], other_tsos, col_name[2])]
    # calculate the sum using columns without NaNs the day
    # before or during the period to be guessed
    similar = similar.dropna(
        axis=1,
        how='any',
        subset=day_before.append(to_fill)
    ).sum(axis=1)
    # calculate scaling factor for other TSO data
    factor = similar.loc[day_before].sum(
        axis=0) / col.loc[day_before, :].sum(axis=0)

    guess = similar.loc[to_fill] / float(factor)
    col.iloc[:, 0].loc[to_fill] = guess
    a = float(col.iloc[:, 0].loc[nan_region['start_idx'] - one_period])
    b = float(col.iloc[:, 0].loc[nan_region['start_idx']])
    if a == 0:
        deviation = '{} absolut'.format(a - b)
    else:
        deviation = '{:.2f} %'.format((a - b) / a * 100)
    logger.info(
        '%s : \n        '
        'guessed %s entries after %s \n        '
        'last non-missing: %s \n        '
        'first guessed: %s \n        '
        'deviation of first guess from last known value: %s',
        col_name[0:3], nan_region['count'], nan_region[
            'start_idx'], a, b, deviation
    )

    return col


def resample_markers(group):
    '''Resample marker column from 15 to 60 min

    Parameters
    ----------
    group: pd.Series
        Series of 4 succeeding quarter-hourly values from the marker column
        that have to be combined into one.

    Returns
    ----------
    aggregated_marker : str or np.nan
        If there were any markers in group: the unique values from the marker
        column group joined together in one string, np.nan otherwise

    '''

    if group.notnull().values.any():
        # unpack string of markers into a list
        unpacked = [mark for line in group if type(line) is str
                    for mark in line.split(' | ')]  # [:-1]]
        # keep only unique values from the list
        aggregated_marker = ' | '.join(set(unpacked))  # + ' | '

    else:
        aggregated_marker = np.nan

    return aggregated_marker
