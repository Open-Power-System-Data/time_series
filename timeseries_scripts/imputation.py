"""
Open Power System Data

Timeseries Datapackage

imputation.py : fill functions for imoutation of missing data (Not used yet)

"""

from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger('log')
logger.setLevel('DEBUG')


def find_nan(frame, headers, patch=False):
    '''
    Search for missing values in a DataFrame and optionally apply further 
    functions on each column.

    Parameters
    ----------    
    frame : pandas.DataFrame
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
        original frame or frame with gaps patched
    nan_table: pandas.DataFrame
        contains information about missing data

    '''
    nan_table = pd.DataFrame()
    patched = pd.DataFrame()
    marker_col = pd.DataFrame('', index=frame.index, columns=['comment'])

    # Get the frequency/length of one period offrame
    one_period = frame.index[1] - frame.index[0]
    for col_name, col in frame.iteritems():
        col = col.to_frame()  # kann man colname wieder an col drankleben?

        # tag all occurences of NaN in the data
        # (but not before first or after last actual entry)

        # skip this column if it has no entries at all
        if col.empty:
            continue

        col['tag'] = (
            (col.index >= col.first_valid_index()) &
            (col.index <= col.last_valid_index()) &
            col.isnull().transpose().as_matrix()
        ).transpose()

        # make another DF to hold info about each region
        nan_regs = pd.DataFrame()

        # first row of consecutive region is a True preceded by a False in tags
        nan_regs['start_idx'] = col.index[
            col['tag'] & ~
            col['tag'].shift(1).fillna(False)]

        # last row of consecutive region is a False preceded by a True
        nan_regs['till_idx'] = col.index[
            col['tag'] & ~
            col['tag'].shift(-1).fillna(False)]

        if not col['tag'].any():
            logger.info('%s : nothing to patch in this column', col_name[0:3])
            col.drop('tag', axis=1, inplace=True)
            nan_idx = pd.MultiIndex.from_arrays([
                [0, 0, 0, 0],
                ['count', 'span', 'start_idx', 'till_idx']])
            nan_list = pd.DataFrame(index=nan_idx, columns=col.columns)

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
                    col, col_name, nan_regs, frame, marker_col, one_period)

        if patched.empty:
            patched = col
        else:
            patched = patched.combine_first(col)

        if nan_table.empty:
            nan_table = nan_list
        else:
            nan_table = nan_table.combine_first(nan_list)

    # append the marker to the DataFrame
    marker_col.replace(to_replace='', value=np.nan, inplace=True)
    tuples = [('marker', '', '', '', '')]
    marker_col.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    # patched = patched.combine_first(marker_col)#.to_frame())
    patched = pd.concat([patched, marker_col], axis=1)  # .to_frame())

    # set the level names for the output
    nan_table.columns.names = headers
    patched.columns.names = headers

    return patched, nan_table


def choose_fill_method(col, col_name, nan_regs, frame, marker_col, one_period):
    '''
    Choose the appropriate function for filling a region of missing values

    Parameters
    ----------  
    col : pandas.DataFrame
        A column from frame as a separate DataFrame 
    col_name : str
        Name of DataFrame to inspect
    nan_regs : : pandas.DataFrame
        DataFrame with each row representing a region of missing data in col
    frame : pandas.DataFrame
        DataFrame to patch with n rows
    marker_col: pandas.DataFrame
        An n*1 DataFrame specifying for each row which of the previously treated 
        columns have been patched
    one_period : pandas.Timedelta
        Time resolution of frame and col (15/60 minutes)

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
            col, marker_col = my_interpolate(i, j, nan_region, col, col_name,
                                             marker_col, nan_regs, one_period)
        # Guess missing value spans longer than one hour based on other tsos
        # (Only for German wind and solar generation data)
        elif col_name[1][:2] == 'DE' and col_name[2] == 'generation':
            j += 1

            # NOT IMPLEMENTED
            # col = impute(nan_region, col, col_name, nan_regs, frame, one_period)

    return col, marker_col


def my_interpolate(i, j, nan_region, col, col_name, marker_col, nan_regs, one_period):
    '''
    Interpolate one missing value region in one column as described by 
    nan_region. See choose_fill_method() for info on other parameters.

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

    Returns
    ----------
    col : pandas.DataFrame
        The column with all nan_regs treated for periods shorter than 1:15.

    '''
    if i + 1 == len(nan_regs):
        logger.info('%s : \n\t '
                    'interpolated %s up-to-2-hour-spans of NaNs',
                    col_name[0:3], i + 1 - j)

    to_fill = slice(nan_region['start_idx'] - one_period,
                    nan_region['till_idx'] + one_period)
    to_comment = slice(nan_region['start_idx'], nan_region['till_idx'])

    col.iloc[:, 0].loc[to_fill] = col.iloc[:, 0].loc[to_fill].interpolate()

    # Create a marker column to mark where data has been interpolated
    marker_col.loc[to_comment] = marker_col + '_'.join(col_name[0:3]) + '; '

    return col, marker_col


# Not implemented: For the generation timeseries, larger gaps are guessed
# by up-/down scaling the data from other balancing areas to fit the
# expected magnitude of the missing data.


def impute(nan_region, col, col_name, nan_regs, frame, one_period):
    '''
    Impute missing value spans longer than one hour based on other TSOs.

    Parameters
    ----------
    nan_region : pandas.Series
        Contains information on one region of missing data in col
    col : pandas.DataFrame
        A column from frame as a separate DataFrame 
    col_name : str
        DataFrame to inspect
    nan_regs : : pandas.DataFrame
        DataFrame with each row representing a region of missing data in col
    frame : pandas.DataFrame
        DataFrame to patch
    one_period : pandas.Timedelta
        Time resolution of frame and col (15/60 minutes)

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

    # other_tsos = [c[1] for c in compact.drop(col_name, axis=1).loc[:,(col_name[0],slice(None),col_name[2])].columns.tolist()]
    other_tsos = [
        tso for tso in ['DE-50Hertz', 'DE-Amprion', 'DE-TenneT', 'DE-TransnetBW']
        if tso != col_name[1]]

    # select columns with data for same technology (wind/solar) but from other
    # TSOs
    similar = frame.loc[:, (col_name[0], other_tsos, col_name[2])]
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
