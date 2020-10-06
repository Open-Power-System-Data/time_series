'''
Open Power System Data

Time series Datapackage

read.py : read time series files

'''
import pytz
import yaml
import os
import sys
import numpy as np
import pandas as pd
import logging
from datetime import datetime, date, time, timedelta
import xlrd
from xml.sax import ContentHandler, parse
from .excel_parser import ExcelHandler

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


def read_entso_e_transparency(
        areas,
        filepath,
        dataset_name,
        headers,
        cols,
        stacked,
        unstacked,
        append_headers,
        **kwargs):
    '''
    Read a .csv file from ENTSO-E TRansparency into a DataFrame.
    Parameters
    ----------
    filepath : str
        Directory path of file to be read
    dataset_name : str
        Name of variable, e.g. ``solar``
    url : str
        URL linking to the source website where this data comes from
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    cols : dict
        A mapping of of columnnames to use from input file and a new name to
        rename them to. The new name is the header level whose corresponding
        values are specified in that column
    stacked : list
        List of strings indicating the header levels that are reported
        column-wise in the input files
    unstacked
        One strings indicating the header level that is reported row-wise in the
        input files
    append_headers: dict
        Map of header levels and values to append to Multiindex
    kwargs: dict
        placeholder for further named function arguments
    Returns
    ----------
    df: pandas.DataFrame
        The content of one file from ENTSO-E Transparency
    '''

    df_raw = pd.read_csv(
        filepath,
        sep='\t',
        encoding='utf-16',
        header=0,
        index_col='timestamp',
        parse_dates={'timestamp': ['DateTime']},
        date_parser=None,
        dayfirst=False,
        decimal='.',
        thousands=None,
        usecols=cols.keys(),
    )

    # rename columns to comply with other data
    df_raw.rename(columns=cols, inplace=True)

    if dataset_name == 'Actual Generation per Production Type':
        # keep only renewables columns
        renewables = {
            'Solar': 'solar',
            'Wind Onshore': 'wind_onshore',
            'Wind Offshore': 'wind_offshore'
        }
        df_raw = df_raw[df_raw['variable'].isin(renewables.keys())]
        df_raw.replace({'variable': renewables}, inplace=True)

    if dataset_name == 'Day-ahead Prices':
        # Omit polish price data reported in EUR (keeping PLN prices)
        # (Before 2017-03-02, the data is very messy)
        no_polish_euro = ~(
            (df_raw['region'] == 'PSE SA BZ') &
            (df_raw.index < pd.to_datetime('2017-03-02 00:00:00')))
        df_raw = df_raw.loc[no_polish_euro]

    if dataset_name in ['Actual Total Load', 'Day-ahead Total Load Forecast']:
    # Zero load is highly unlikely. Such occurences are actually NaNs
        df_raw['load'].replace(0, np.nan, inplace=True)

    # keep only entries for selected geographic entities as specified in
    # areas.csv
    area_filter = areas['primary AreaName ENTSO-E'].dropna()
    df_raw = df_raw.loc[df_raw['region'].isin(area_filter)]

    # based on the AreaName column, map the area names used throughout OPSD
    lookup = areas.set_index('primary AreaName ENTSO-E')['area ID'].dropna()
    lookup = lookup[~lookup.index.duplicated()]
    df_raw['region'] = df_raw['region'].map(lookup)

    dfs = {}
    for res in ['15', '30', '60']:
        df = (df_raw.loc[df_raw['resolution'] == 'PT' + res + 'M', :]
                    .copy().sort_index(axis='columns'))

        df = df.drop(columns=['resolution'])

        # DST-handling
        # Hours 2-3 of the DST-day in March are both labelled 3:00, with no possibility
        # to distinguish them. We have to delete both
        dst_transitions_spring = [d.replace(hour=3, minute=m)
            for d in pytz.timezone('Europe/Paris')._utc_transition_times
                if 2000 <= d.year <= datetime.today().year and d.month == 3
                for m in [0, 15, 30, 45]]

        df = df.loc[~df.index.isin(dst_transitions_spring)]

        # juggle the index and columns
        df.set_index(stacked, append=True, inplace=True)
        # at this point, only the values we are intereseted in are left as
        # columns
        df.columns.rename(unstacked, inplace=True)
        df = df.unstack(stacked)

        # keep only columns that have at least some nonzero values
        df = df.loc[:, (df > 0).any(axis=0)]

        # add source, url and unit to the column names.
        # Note: pd.concat inserts new MultiIndex values infront of the old ones
        df = pd.concat([df],
                       keys=[tuple(append_headers.values())],
                       names=append_headers.keys(),
                       axis='columns')

        # reorder and sort columns
        df = df.reorder_levels(headers, axis=1)

        dfs[res + 'min'] = df

    return dfs


def read_pse(filepath):
    '''
    Read a .csv file from PSE into a DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read
    url : str
        URL linking to the source website where this data comes from
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe

    Returns
    ----------
    df: pandas.DataFrame
        The content of one file from PSE

    '''

    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='cp1250',
        header=0,
        index_col=None,
        parse_dates=None,
        date_parser=None,
        dayfirst=False,
        decimal=',',
        thousands=None,
        # hours are indicated by their ending time. During fall DST,
        # UTC 23:00-00:00 = CEST 1:00-2:00 is indicated by '02',
        # UTC 00:00-01:00 = CEST 2:00-3:00 is indicated by '02A',
        # UTC 01:00-02:00 = CET  2:00-3:00 is indicated by '03'.
        # regular hours require backshifting by 1 period
        converters={
            'Time': lambda x: '2:00' if x == '2A' else str(int(x) - 1) + ':00'
        }
    )
    # Create a list of spring-daylight savings time (DST)-transitions
    dst_transitions_spring = [
        d.replace(hour=2)
        for d in pytz.timezone('Europe/Warsaw')._utc_transition_times
        if 2000 <= d.year <= datetime.today().year and d.month == 3]

    # Account for an error where an hour is jumped in the data, incrementing
    # the hour by one
    #time_int = df['Time'].str[:-3].astype(int)
    # if (time_int time_int.shift(1) - 1).
    # if (time_int == 24).any():
    #    logger.info(filepath)
    #    df = df[time_int != 24]
    if df['Date'][0] == 20130324:
        df['Time'] = [str(num) + ':00' for num in range(24)]

    # The hour from 01:00 - 02:00 (CET) should by PSE's logic be indexed
    # by "02:00" (the endpoint), but at DST day in spring they use "03:00" in
    # the files. Our routine requires it to be "01:00" (the start point).
    df['proto_timestamp'] = pd.to_datetime(
        df['Date'].astype(str) + ' ' + df['Time'])
    slicer = df['proto_timestamp'].isin(dst_transitions_spring)
    df.loc[slicer, 'Time'] = '1:00'

    # create the actual timestamp from the corrected "Date"-column
    df.index = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'])

    # DST-handling
    # 'ambiguous' refers to how the October dst-transition hour is handled.
    # 'infer' will attempt to infer dst-transition hours based on order.
    df.index = df.index.tz_localize('Europe/Warsaw', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    return df


def read_ceps(filepath):
    '''Read a file from CEPS into a DataFrame'''
    df = pd.read_csv(
        filepath,
        sep=';',
        header=2,
        parse_dates=True,
        dayfirst=True,
        skiprows=None,
        index_col=0,
        usecols=[0, 1, 2]
    )

    # DST-handling
    df.index = df.index.tz_localize('Europe/Prague', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    return df


def read_elia(filepath):
    '''Read a file from Elia into a DataFrame'''
    df = pd.read_excel(
        io=filepath,
        header=3,
        parse_dates={'timestamp': ['DateTime']},
        dayfirst=True,
        index_col='timestamp',
        usecols=None
    )

    # DST handling
    df.index = df.index.tz_localize('Europe/Brussels', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    return df


def read_energinet_dk(filepath):
    '''Read a file from energinet.dk into a DataFrame'''
    df = pd.read_excel(
        io=filepath,
        header=2,  # the column headers are taken from 3rd row.
        # 2nd row also contains header info like in a multiindex,
        # i.e. wether the colums are price or generation data.
        # However, we will make our own columnnames below.
        # Row 3 is enough to unambiguously identify the columns
        skiprows=None,
        index_col=None,
        parse_dates=True,
        dayfirst=False,
        usecols=None,  # None means: parse all columns
        thousands=',',
        # hours in 2nd column run from 1-24, we need 0-23:
        # (converters seem not to work in combination with parse_dates)
        converters={1: lambda x: x - 1}
    )

    # Create the timestamp column and set as index
    df.index = df.iloc[:, 0] + pd.to_timedelta(df.iloc[:, 1], unit='h')

    # DST-handling
    # Create a list of spring-daylight savings time (DST)-transitions
    dst_transitions_spring = [
        d.replace(hour=2)
        for d in pytz.timezone('Europe/Copenhagen')._utc_transition_times
        if 2000 <= d.year <= datetime.today().year and d.month == 3]

    # Drop 3rd hour for (spring) DST-transition from df.
    df = df[~df.index.isin(dst_transitions_spring)]

    # Verify that daylight savings time transitions are handled as expected
    check_dst(df.index, autumn_expect=1)

    # Conform index to UTC
    dst_arr = np.ones(len(df.index), dtype=bool)
    df.index = df.index.tz_localize('Europe/Copenhagen', ambiguous=dst_arr)
    df.index = df.index.tz_convert(None)

    return df


def read_entso_e_statistics(filepath,):
    '''Read a file from ENTSO-E into a DataFrame'''
    df = pd.read_excel(
        io=filepath,
        header=18,
        usecols='A, B, G, K, L, N, P:AU'
    )
    # rename columns
    # According to the specific national considerations, GB data reflects the
    # whole UK including Northern Ireland since 2016
    renamer = {df.columns[0]: 'date', df.columns[1]: 'time', 'GB': 'GB_UKM'}
    df.rename(columns=renamer, inplace=True)

    # Zero load is highly unlikely. Such occurences are actually NaNs
    df.replace(0, np.nan, inplace=True)

    # Construct the index and set timezone
    # for some reason, the 'date' column has already been parsed to datetime
    df['date'] = df['date'].fillna(method='ffill').dt.strftime('%Y-%m-%d')
    df.index = pd.to_datetime(df.pop('date') + ' ' + df.pop('time').str[:5])

    # DST-handling
    df.index = df.index.tz_localize('Europe/Brussels', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    return df


def read_entso_e_portal(filepath):
    '''Read a file from the old ENTSO-E Data Portal into a DataFrame'''
    df = pd.read_excel(
        io=filepath,
        header=3,  # 0 indexed, so the column names are actually in the 4th row
        skiprows=None,
        # create MultiIndex from first 2 columns ['date', 'Country']
        index_col=[0, 1],
        parse_dates={'date': ['Year', 'Month', 'Day']},
        dayfirst=False,
        usecols=None,  # None means: parse all columns
    )

    # The "Coverage ratio"-column specifies for some countries scaling factor
    # with which we should upscale the reported values
    df = df.divide(df.pop('Coverage ratio'), axis='index') * 100

    # The original data has days and countries in the rows and hours in the
    # columns.  This rearranges the table, mapping hours on the rows and
    # countries on the columns.
    df.columns.names = ['hour']
    df = df.stack(level='hour').unstack(level='Country').reset_index()

    # Create the timestamp column and set as index
    df.index = df.pop('date') + pd.to_timedelta(df.pop('hour'), unit='h')

    # DST-handling
    # Delete values in DK and FR that should not exist
    df = df.loc[df.index != '2015-03-29 02:00', :]

    # Delete values in DK that are obviously twice as high as they should be
    df.loc[df.index.isin(['2014-10-26 02:00:00', '2015-10-25 02:00:00']),
           'DK'] = np.nan

    # Delete values in UK that are all zero except for one day
    df.loc[(df.index.year == 2010) & (df.index.month == 1), 'GB'] = np.nan

    # Delete values in CY that are mostly zero but not always
    df.loc[(df.index.year < 2013), 'CY'] = np.nan

    # Zero load is highly unlikely. Such occurences are actually NaNs
    df.replace(0, np.nan, inplace=True)

    # Verify that daylight savings time transitions are handled as expected
    check_dst(df.index, autumn_expect=1)
    # Conform index to UTC
    dst_arr = np.ones(len(df.index), dtype=bool)
    df.index = df.index.tz_localize('CET', ambiguous=dst_arr)
    df.index = df.index.tz_convert(None)

    # Rename regions to comply with naming conventions
    renamer = {'DK_W': 'DK_1', 'UA_W': 'UA_west', 'NI': 'GB_NIR', 'GB': 'GB_GBN'}
    df.rename(columns=renamer, inplace=True)

    # Calculate load for whole UK from Great Britain and Northern Ireland data
    df['GB_UKM'] = df['GB_GBN'].add(df['GB_NIR'])

    return df


def read_hertz(filepath, dataset_name):
    '''Read a file from 50Hertz into a DataFrame'''
    df = pd.read_csv(
        filepath,
        sep=';',
        header=3,
        index_col='timestamp',
        parse_dates={'timestamp': ['Datum', 'Von']},
        date_parser=None,
        dayfirst=True,
        decimal=',',
        thousands='.',
        # truncate values in 'time' column after 5th character
        converters={'Von': lambda x: x[:5]},
    )

    # Wind onshore
    if dataset_name == 'wind generation_actual pre-offshore':
        df['wind_onshore'] = df['MW']

    # Until 2006, and in 2015 (except for wind_generation_pre-offshore),
    # during the fall dst-transistion, only the
    # wintertime hour (marked by a B in the data) is reported, the summertime
    # hour, (marked by an A) is missing in the data.
    # dst_arr is a boolean array consisting only of "False" entries, telling
    # python to treat the hour from 2:00 to 2:59 as wintertime.

    # Verify that daylight savings time transitions are handled as expected
    check_dst(df.index, autumn_expect=2)
    # Conform index to UTC
    if (pd.to_datetime(df.index.values[0]).year not in [2005, 2006, 2015] or
            (dataset_name == 'wind generation_actual pre-offshore' and
             pd.to_datetime(df.index.values[0]).year == 2015)):
        check_dst(df.index, autumn_expect=2)
        df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    else:
        dst_arr = np.zeros(len(df.index), dtype=bool)
        check_dst(df.index, autumn_expect=1)
        df.index = df.index.tz_localize('Europe/Berlin', ambiguous=dst_arr)

    df.index = df.index.tz_convert(None)

    variable, attribute = dataset_name.split(' ')[:2]

    # Since 2016, wind data has an aditional column for offshore.
    # Baltic 1 has been producing since 2011-05-02 and Baltic2 since
    # early 2015 (source: Wikipedia) so it is probably not correct that
    # 50Hertz-Wind data pre-2016 is only onshore. Maybe we can ask at
    # 50Hertz directly.

    return df


def read_amprion(filepath, dataset_name):
    '''Read a file from Amprion into a DataFrame'''
    df = pd.read_csv(
        filepath,
        sep=';',
        header=0,
        index_col='timestamp',
        parse_dates={'timestamp': ['Datum', 'Uhrzeit']},
        date_parser=None,
        dayfirst=True,
        decimal=',',
        thousands=None,
        # Truncate values in 'time' column after 5th character.
        converters={'Uhrzeit': lambda x: x[:5]},
    )

    # Wind onshore
    if dataset_name == 'wind':
        df['wind_onshore'] = df['Online Hochrechnung [MW]']

    # DST-Handling:
    # In the years after 2009, during the fall dst-transistion, only the
    # summertime hour is reported, the wintertime hour is missing in the data.
    # dst_arr is a boolean array consisting only of "True" entries, telling
    # python to treat the hour from 2:00 to 2:59 as summertime.

    # Verify that daylight savings time transitions are handled as expected
    check_dst(df.index, autumn_expect=0)

    index1 = df.index[df.index.year >= 2018]
    index1 = index1.tz_localize('Europe/Berlin', ambiguous='infer')
    index2 = df.index[df.index.year < 2018]
    dst_arr = np.ones(len(index2), dtype=bool)
    index2 = index2.tz_localize('Europe/Berlin', ambiguous=dst_arr)
    df.index = index2.append(index1)
    df.index = df.index.tz_convert(None)

    return df


def read_tennet(filepath, dataset_name):
    '''Read a file from TenneT into a DataFrame'''
    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='latin_1',
        header=3,
        index_col=False,
        parse_dates=False,
        date_parser=None,
        dayfirst=True,
        thousands=None,
        converters=None,
    )

    # Wind onshore
    if dataset_name == 'wind':
        # Calculate onshore wind-generation
        df['wind_onshore'] = df['tatsächlich [MW]'].sub(
            df['Anteil Offshore [MW]'])

    # Construct the datetime-inex
    renamer = {'Datum': 'date', 'Position': 'pos'}
    df.rename(columns=renamer, inplace=True)
    df['date'].fillna(method='ffill', limit=100, inplace=True)

    # Check the rows for irregularities
    for i, row in df.iterrows():
        # there must not be more than 100 quarter-hours in a day
        if row['pos'] > 100:
            logger.warning('%s th quarter-hour at %s, position %s',
                           row['pos'], row['date'], i)

        # On the day in March when summertime begins, shift the data forward by
        # 1 hour, beginning with the 9th quarter-hour, so the index runs again
        # up to 96
        elif (row['pos'] == 92 and (
                (i == len(df.index) - 1) or (df['pos'][i + 1] == 1))):
            slicer = df[(df['date'] == row['date']) & (df['pos'] >= 9)].index
            df.loc[slicer, 'pos'] = df['pos'] + 4

        # Instead of having the quarter-hours' index run up to 100, we want
        # to have it set back by 1 hour beginning from the 13th
        # quarter-hour, ending at 96
        elif row['pos'] == 100:  # True when summertime ends in October
            slicer = df[(df['date'] == row['date']) & (df['pos'] >= 13)].index
            df.loc[slicer, 'pos'] = df['pos'] - 4

    # Compute timestamp from position and generate datetime-index
    df['hour'] = (np.trunc((df['pos'] - 1) / 4)).astype(int).astype(str)
    df['minute'] = (((df['pos'] - 1) % 4) * 15).astype(int).astype(str)
    df.index = pd.to_datetime(
        df['date'] + ' ' + df['hour'] + ':' + df['minute'], dayfirst=True)

    # DST-handling
    df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    return df


def read_transnetbw(filepath, dataset_name):
    '''Read a file from TransnetBW into a DataFrame'''
    df = pd.read_csv(
        filepath,
        sep=';',
        header=0,
        index_col=None,
        parse_dates=None,  # {'timestamp': ['Datum von', 'Uhrzeit von']},
        date_parser=None,
        dayfirst=True,
        decimal=',',
        thousands=None,
        converters=None,
    )

    # Wind onshore
    if dataset_name == 'wind':
        df['wind_onshore'] = df['Ist-Wert (MW)']

    # rename columns
    renamer = {'Datum von': 'date', 'Uhrzeit von': 'time'}
    df.rename(columns=renamer, inplace=True)

    # DST-handling
    # timestamp 01:45 just before spring DST transistion has been falsely set to
    # 3:45, which we correct here
    slicer = (df['time'] == '03:45') & (df['time'].shift(periods=1) == '01:30')
    df.loc[slicer, 'time'] = '01:45'
    df.index = pd.to_datetime(df['date'] + ' ' + df['time'], dayfirst=True)

    dst_arr = np.zeros(len(df.index), dtype=bool)
    df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    return df


def read_opsd(filepath, param_dict, headers):
    '''Read a file from OPSD into a DataFrame'''
    df = pd.read_csv(
        filepath,
        sep=',',
        header=0,
        index_col='timestamp',
        parse_dates={'timestamp': ['day']},
        date_parser=None,
        dayfirst=False,
        decimal='.',
        thousands=None,
        converters=None,
    )

    # Split the colname after the first "_"
    cols = [(col_name.split('_')[0], '_'.join(col_name.split('_')[1:-1]))
            for col_name in df.columns]
    df.columns = pd.MultiIndex.from_tuples(cols)

    # Keep only wind and solar
    keep = ['wind', 'wind_onshore', 'wind_offshore', 'solar']
    df = df.loc[:, (slice(None), keep)]

    # The capacities data only has one entry per day, which pandas
    # interprets as 00:00h. We will broadcast the dayly data for
    # all quarter-hours of the day until the next given data point.
    # For this, we we expand the index so it reaches to 23:59 of
    # the last day, not only 00:00.
    last = pd.to_datetime([df.index[-1]]) + timedelta(days=1, minutes=59)
    until_last = df.index.append(last).rename('timestamp')
    df = df.reindex(index=until_last, method='ffill')
    df = df.loc[(2005 <= df.index.year) & (df.index.year <= 2019)]

    dfs = {}
    for timezone, res, ddf in [
        ('CET', '15min', df.loc[:, ['DE']]),
        ('WET', '30min', df.loc[:, ['GB-UKM', 'GB-GBN', 'GB-NIR']]),
        ('CET', '60min', df.loc[:, ['CH', 'DK', 'SE']])]:

        # DST-handling
        ddf.index = ddf.index.tz_localize(timezone).tz_convert(None)
        ddf = ddf.resample(res).ffill().round(0)

        # Create the MultiIndex
        cols = [tuple(param_dict['colmap'][col_name[0]][level]
                      .format(variable=col_name[1].lower())
                      for level in headers) for col_name in ddf.columns]
        ddf.columns = pd.MultiIndex.from_tuples(cols, names=headers)
        dfs[res] = ddf

    return dfs


def read_svenska_kraftnaet(filepath, dataset_name):
    '''Read a file from Svenska Kraftnät into a DataFrame'''
    if dataset_name in ['wind_solar_1', 'wind_solar_2']:
        skip = 4
        cols = {0: 'date', 1: 'hour', 2: 'load', 3: 'wind'}
    else:
        if dataset_name == 'wind_solar_4':
            skip = 5
        else:
            skip = 7
        cols = {0: 'timestamp', 1: 'load', 2: 'wind', 8: 'solar'}

    df = pd.read_excel(
        io=filepath,
        # read the last sheet (in some years,
        # there are hidden sheets that would cause errors)
        sheet_name=-1,
        header=None,
        skiprows=skip,
        index_col=None,
        usecols=cols.keys(),
        names=cols.values()
    )

    if dataset_name in ['wind_solar_1', 'wind_solar_2']:
        # in 2009 there is a row below the table for the sums that we don't
        # want to read in
        df = df[df['date'].notnull()]
        df.index = pd.to_datetime(
            df['date'].astype(int).astype(str) + ' ' +
            df['hour'].astype(int).astype(str).str.replace('00', '') + ':00',
            dayfirst=False,
            infer_datetime_format=True)

    else:
        # in 2011 there is a row below the table for the sums that we don't
        # want to read in
        df = df[((df['timestamp'].notnull()) &
                 (df['timestamp'].astype(str) != 'Tot summa GWh'))]
        df.index = pd.to_datetime(df['timestamp'], dayfirst=True)

    # The timestamp ("Tid" in the original) gives the time without
    # daylight savings time adjustments (normaltid). To convert to UTC,
    # one hour has to be deducted
    df.index = df.index - timedelta(hours=1)  # + pd.offsets.Hour(-1)

    return df


def read_apg(filepath):
    '''Read a file from APG into a DataFrame'''
    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='latin_1',
        header=0,
        index_col='timestamp',
        parse_dates={'timestamp': ['Von']},
        dayfirst=True,
        decimal=',',
        thousands='.',
        # Format of the raw_hour-column is normally is 01:00:00, 02:00:00 etc.
        # throughout the year, but 3A:00:00, 3B:00:00 for the (possibly
        # DST-transgressing) 3rd hour of every day in October, we truncate the
        # hours column after 2 characters and replace letters which are there to
        # indicate the order during fall DST-transition.
        converters={'Von': lambda x: str(x).replace('A', '').replace('B', '')}
    )

    # Correct column names
    df.rename(columns=lambda x: x.replace('  ', ' '), inplace=True)

    # DST-handling
    df.index = df.index.tz_localize('Europe/Vienna', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    return df


def read_rte(filepath):
    '''Read a file from RTE into a DataFrame'''
    cols = ['Date', 'Heure', 'Consommation (MW)', 'Prévision J-1 (MW)',
            'Eolien (MW)', 'Solaire (MW)']
    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='utf-8',
        header=0,
        index_col='timestamp',
        # there eis also a column with UTC but it is incorrect
        parse_dates={'timestamp': ['Date', 'Heure']},
        dayfirst=True,
        usecols=cols
    )

    #  filter out quarter-hourly oberservations and sort the index
    df = df.loc[df.index.minute.isin([0, 30]), :]
    df.sort_index(axis='index', inplace=True)

    # DST handling
    # drop 1 hour after spring dst as it contains inconsistent data (copy of
    # hour before). The 1 hour will later be interpolated
    dst_transitions_spring = [
        dd for d in pytz.timezone('Europe/Paris')._utc_transition_times
        if 2000 <= d.year <= datetime.today().year and d.month == 3
        for dd in (d.replace(hour=2, minute=0), d.replace(hour=2, minute=30))]
    df = df.loc[~df.index.isin(dst_transitions_spring)]

    # Verify that daylight savings time transitions are handled as expected
    check_dst(df.index, autumn_expect=1)
    # Conform index to UTC
    dst_arr = np.zeros(len(df.index), dtype=bool)
    df.index = df.index.tz_localize('Europe/Paris', ambiguous=dst_arr)
    df.index = df.index.tz_convert(None)

    return df


def read_GB(filepath):
    '''Read a file from National Grid or Elexon into a DataFrame'''
    time_cols = {
        '#Settlement Date': 'date',  # Elexon
        'Settlement Period': 'pos',  # Elexon
        'SETTLEMENT_DATE': 'date',   # National Grid
        'SETTLEMENT_PERIOD': 'pos'   # National Grid
    }

    df = pd.read_csv(
        filepath,
        header=0,
        usecols=None,
        dayfirst=True
    )

    df.rename(columns=time_cols, inplace=True)

    for i, row in df.iterrows():
        # there must not be more than 50 half-hours in a day
        if row['pos'] > 50:
            logger.warning('%s th half-hour at %s, position %s',
                           row['pos'], row['date'], i)

        # On the day in March when summertime begins, shift the data forward by
        # 1 hour, beginning with the 5th half-hour, so the index runs again
        # up to 48
        elif (row['pos'] == 46 and (
                (i == len(df.index) - 1) or (df['pos'][i + 1] == 1))):
            slicer = df[(df['date'] == row['date']) & (df['pos'] >= 3)].index
            df.loc[slicer, 'pos'] = df['pos'] + 2

        # Instead of having the half-hours' index run up to 50, we want
        # to have it set back by 1 hour beginning from the 5th
        # half-hour, ending at 48
        elif row['pos'] == 50:  # True when summertime ends in October
            slicer = df[(df['date'] == row['date']) & (df['pos'] >= 5)].index
            df.loc[slicer, 'pos'] = df['pos'] - 2

    # Compute timestamp from position and generate datetime-index
    df['hour'] = (np.trunc((df['pos'] - 1) / 2)).astype(int).astype(str)
    df['minute'] = (((df['pos'] - 1) % 2) * 30).astype(int).astype(str)
    df.index = pd.to_datetime(
        df['date'] + ' ' + df['hour'] + ':' + df['minute'], dayfirst=True)

    # DST-handling
    df.index = df.index.tz_localize('Europe/London', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    return df


def terna_file_to_initial_dataframe(filepath):
    '''
    Parse the xml or read excel directly, 
    returning the data from the file in a simple-index dataframe.

    Some files are formated as xml, some are pure excel files.
    This function handles both cases.

    Parameters:
    ----------
    filepath: str 
        The path of the file to process

    Returns:
    ----------
    df: pandas.DataFrame
        A pandas dataframe containing the data from the specified file.

    '''
    # First, we'll try to parse the file as if it is xml.
    try:
        excelHandler = ExcelHandler()
        parse(filepath, excelHandler)

        # Create the dataframe from the parsed data
        df = pd.DataFrame(excelHandler.tables[0][2:],
                          columns=excelHandler.tables[0][1])

        # Convert the "Generation [MWh]"-column to numeric
        df['Generation [MWh]'] = pd.to_numeric(df['Generation [MWh]'])
    except:
        # In the case of an exception, treat the file as excel.
        try:
            df = pd.read_excel(filepath, header=1)
        except xlrd.XLRDError:
            df = pd.DataFrame()

    return df


def read_terna(filepath, filedate, param_dict, headers):
    '''
    Read a file from Terna into a dataframe

    Parameters:
    ----------
    filepath: str
        The path of the file to read.
    url:
        The url of the Terna page.
    headers:
        Levels for the MultiIndex.

    Returns:
    ----------
    df: pandas.DataFrame
        A pandas multi-index dataframe containing the data from the specified
        file.

    '''

    # Reading the file into a pandas dataframe
    df = terna_file_to_initial_dataframe(filepath)

    if df.empty:
        return df

    # Rename columns to match conventions
    renamer = {
        'Date/Hour': 'timestamp',
        'Bidding Area': 'region',
        'Type': 'variable',
        'Generation [MWh]': 'values'
    }
    df.rename(columns=renamer, inplace=True)

    # Casting the timestamp column to datetime and set it as index
    df.index = pd.to_datetime(df['timestamp'])

    # Some files contain data for different date than they should, in which
    # case the link to the file had a different date than what we see after
    # opening the file. So for the day they are supposed to represent there is
    # no data and for the day they contain there is duplicate data.
    # We skip these files alltogether.
    if not (df.index.date == filedate).all():
        return pd.DataFrame()

    # Renaming the bidding area names to conform to the codes from areas.csv
    df['region'] = 'IT_' + df['region']

    # Renaming and filtering out wind and solar
    # "PV Estimated" are solar panels connected to the distribution grid
    # "PV Measured" are those connected to transmission grid
    renewables = {
        'Wind': ('wind_onshore', 'generation_actual'),
        'Photovoltaic Estimated': ('solar', 'generation_actual_tso'),
        'Photovoltaic Measured': ('solar', 'generation_actual_dso')
    }

    df = df.loc[df['variable'].isin(renewables.keys()), :]

    for k, v in renewables.items():
        df.loc[df['variable'] == k, 'attribute'] = v[1]
        df.loc[df['variable'] == k, 'variable'] = v[0]

    # Reshaping the data so that each combination of a bidding area and type
    # is represented as a column of its own.
    stacked = ['region', 'variable', 'attribute']
    df.set_index(stacked, append=True, inplace=True)
    df = df['values'].unstack(stacked)

    # DST-handling
    # Verify that daylight savings time transitions are handled as expected
    check_dst(df.index, autumn_expect=2)
    # drop autumn dst hours as they contain inconsistent data
    # (apparently 2:00 and 3:00 are added up and reported as value for 2:00).
    # The 2 hours will later be interpolated
    dst_transitions_autumn = [
        d.replace(hour=2)
        for d in pytz.timezone('Europe/Rome')._utc_transition_times
        if 2000 <= d.year <= datetime.today().year and d.month == 10]
    df = df.loc[~df.index.isin(dst_transitions_autumn)]

    # Covert to UTC
    df.index = df.index.tz_localize('Europe/Rome')
    df.index = df.index.tz_convert(None)

    # Create the MultiIndex
    cols = [tuple(param_dict['colmap'][level]
                  .format(region=col_name[0],
                          variable=col_name[1],
                          attribute=col_name[2])
            for level in headers) for col_name in df.columns]
    df.columns = pd.MultiIndex.from_tuples(cols, names=headers)

    # # add source and url to the columns.
    # append_headers = {'source': 'Terna', 'unit': 'MW'}
    # # Note: pd.concat inserts new MultiIndex values infront of the old ones
    # df = pd.concat([df],
    #                keys=[tuple([*append_headers.values(), url])],
    #                names=[*append_headers.keys(), 'web'],
    #                axis='columns')

    # # reorder and sort columns
    # df = df.reorder_levels(headers, axis=1)
    # df.sort_index(axis='columns', inplace=True)

    # # Common headers for calculated colmns
    # c_h = ('own calculation from Terna', url, 'MW')

    # # Italian bidding zones
    # bz_it = ['IT_CNOR', 'IT_CSUD', 'IT_NORD', 'IT_SARD', 'IT_SICI', 'IT_SUD']

    # # Aggregate DSO and TSO level measurements for solar
    # for region in bz_it:
    #     #import pdb; pdb.set_trace()

    #     df[(region, 'solar', 'generation_actual', *c_h)] = (
    #         df[(region, 'solar', 'generation_actual_dso')] +
    #         df[(region, 'solar', 'generation_actual_tso')])

    # # Aggregate all regions
    # for variable in ['solar', 'wind_onshore']:
    #     #for attribute in
    #     df[('IT', variable, 'generation_actual', *c_h)] = (
    #         df.loc[:, (bz_it, variable, 'generation_actual')]
    #         .sum(axis='columns', skipna=True))

    return df


def read(
        sources,
        data_path,
        parsed_path,
        areas,
        headers,
        start_from_user,
        end_from_user,
        testmode=False):

    # For each source in the source dictionary
    for source_name, source_dict in sources.items():
        # For each dataset from source_name
        for dataset_name, param_dict in source_dict.items():
            if source_name == 'Svenska Kraftnaet':
                param_dict['colmap'] = source_dict['wind_solar_1']['colmap']

            read_dataset(
                source_name,
                dataset_name,
                param_dict,
                data_path,
                parsed_path,
                areas,
                headers,
                start_from_user=start_from_user,
                end_from_user=end_from_user,
                testmode=testmode)
    return


def read_dataset(
        source_name,
        dataset_name,
        param_dict,
        data_path,
        parsed_path,
        areas,
        headers,
        start_from_user=None,
        end_from_user=None,
        testmode=False):
    '''
    For the sources specified in the sources.yml file, pass each downloaded
    file to the correct read function.

    Parameters
    ----------
    source_name : str
        Name of source to read files from
    dataset_name : str
        Indicator for subset of data available together in the same files
    param_dict : dict
        Dictionary of further parameters, i.e. the URL of the Source to be
        placed in the column-MultiIndex
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    data_path : str, default: 'original_data'
        Base download directory in which to save all downloaded files
    parsed_path : str
        Directory where to store parsed data as pickle files
    areas : pandas.DataFrame
        Contains mapping of available geographical areas showing how
        countries, bidding zones, control areas relate to each other
    start_from_user : datetime.date, default None
        Start of period for which to read the data
    end_from_user : datetime.date, default None
        End of period for which to read the data
    testmode : bool
        If True, only read one file per source. Use for testing purposes.

    Returns
    ----------
    data_set: pandas.DataFrame
        A DataFrame containing the combined data for dataset_name 

    '''

    # The cumulated dict will store parsed data from all files from one dataset
    cumulated = {'15min': pd.DataFrame(),
                 '30min': pd.DataFrame(),
                 '60min': pd.DataFrame()}

    dataset_dir = os.path.join(data_path, source_name, dataset_name)

    logger.info(' {:20.20} | {:20.20} | reading...'
                .format(source_name, dataset_name))

    files_existing = sum([len(files) for r, d, files in os.walk(dataset_dir)])
    files_success = 0

    # Check if there are files for dataset_name
    if files_existing == 0:
        logger.warning('no files found')
        return

    # Check if there are folders for dataset_name
    if not os.path.exists(dataset_dir):
        logger.warning('folder not found')
        return

    # For each file downloaded for that dataset
    for container in sorted(os.listdir(dataset_dir)):
        files = os.listdir(os.path.join(dataset_dir, container))

        # Skip this file if period covered excluded by user
        if start_from_user:
            # start lies after file end => filecontent is too old
            if start_from_user > yaml.full_load(container.split('_')[1]):
                continue  # go to next container

        if end_from_user:
            # end lies before file start => filecontent is too recent
            if end_from_user < yaml.full_load(container.split('_')[0]) - timedelta(days=1):
                continue  # go to next container

        # Check if there is only one file per folder
        if len(files) == 0:
            logger.warning(container + 'no file found')
            continue

        elif len(files) > 1:
            logger.warning(container + '> 1 file found')
            continue

        filepath = os.path.join(dataset_dir, container, files[0])

        # Check if file is not empty
        if os.path.getsize(filepath) < 128:
            logger.warning(container + 'file too small')
            continue

        # First call to update_progress
        update_progress(files_success, files_existing, container)

        # Select read function for source
        if source_name == 'OPSD':
            parsed = read_opsd(filepath, param_dict, headers)
        elif source_name == 'CEPS':
            parsed = {'60min': read_ceps(filepath)}
        elif source_name == 'ENTSO-E Transparency FTP':
            parsed = read_entso_e_transparency(
                areas, filepath, dataset_name, headers, **param_dict)
        elif source_name == 'ENTSO-E Data Portal':
            parsed = {'60min': read_entso_e_portal(filepath)}
        elif source_name == 'ENTSO-E Power Statistics':
            parsed = {'60min': read_entso_e_statistics(filepath)}
        elif source_name == 'Energinet.dk':
            parsed = {'60min': read_energinet_dk(filepath)}
        elif source_name == 'Elia':
            parsed = {'15min': read_elia(filepath)}
        elif source_name == 'PSE':
            parsed = {'60min': read_pse(filepath)}
        elif source_name == 'RTE':
            parsed = {'30min': read_rte(filepath)}
        elif source_name == 'Svenska Kraftnaet':
            parsed = {'60min': read_svenska_kraftnaet(filepath, dataset_name)}
        elif source_name == '50Hertz':
            parsed = {'15min': read_hertz(filepath, dataset_name)}
        elif source_name == 'Amprion':
            parsed = {'15min': read_amprion(filepath, dataset_name)}
        elif source_name == 'TenneT':
            parsed = {'15min': read_tennet(filepath, dataset_name)}
        elif source_name == 'TransnetBW':
            parsed = {'15min': read_transnetbw(filepath, dataset_name)}
        elif source_name == 'APG':
            parsed = {'15min': read_apg(filepath)}
        elif source_name == 'Terna':
            # Files from 2010-2011 are in tsv format, we ignore them
            filedate = datetime.strptime(
                filepath.split(os.sep)[-1].split('_')[0], '%Y-%m-%d').date()
            if filedate < date(2011, 2, 1):
                continue
            else:
                parsed = {'60min': read_terna(
                    filepath, filedate, param_dict, headers)}
        elif source_name in ['Elexon', 'National Grid']:
            parsed = {'30min': read_GB(filepath)}

        # combine with previously parsed DataFrames from same dataset and same
        # resolution
        for res_key, df in parsed.items():
            if res_key in param_dict['resolution'] and df.empty:
                logger.info('%s | %s | empty DataFrame: ', files[0], res_key)
                continue

            if cumulated[res_key].empty:
                cumulated[res_key] = df
            else:
                cumulated[res_key] = cumulated[res_key].combine_first(df)

        files_success += 1
        update_progress(files_success, files_existing, container)
        if testmode:
            break

    if all([df.empty for df in cumulated.values()]):
        logger.warning(' {:20.20} | {:20.20} | All empty DataFrames'
                       .format(source_name, dataset_name))
        return

    # Finally, create the MultiIndex, trim the DataFrames and save them on disk
    for res_key, df in cumulated.items():
        if df.empty:
            continue
        elif source_name in ['ENTSO-E Transparency FTP', 'Terna', 'OPSD']:
            pass
        else:
            if source_name in ['ENTSO-E Data Portal', 'ENTSO-E Power Statistics']:
                colmap = {col: param_dict['colmap'] for col in df.columns}
            else:
                colmap = param_dict['colmap']
            df = make_multiindex(df, colmap, headers)

        df = trim_df(
            df,
            res_key,
            source_name,
            dataset_name,
            start_from_user,
            end_from_user)

        filename = '_'.join([res_key, source_name, dataset_name]) + '.pickle'
        df.to_pickle(os.path.join(parsed_path, filename))

    return


def trim_df(
        df0,
        res_key,
        source_name,
        dataset_name,
        start_from_user=None,
        end_from_user=None):
    '''
    Reindex a DataFrame with a new index that is sure to be continuous in order
    to expose gaps in the data and cut off data outside the required period

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to be trimmed
    res_key : str
        Resolution of the source data. Must be one of ['15min', '30min', 60min']
    source_name : str
        Name of the source
    dataset_name : str
        Name of the dataset
    Returns
    ----------
    None    
    '''

    # sort the index
    df0.sort_index(axis='index', inplace=True)
    # Reindex with a new index that is sure to be continous in order to later
    # expose gaps in the data.
    #no_gaps = pd.date_range(start=df0.index[0],
    #                        end=df0.index[-1],
    #                        freq=res_key)
    df = df0.asfreq(res_key)
    missing_rows = df.shape[0] - df0.shape[0]
    if not missing_rows == 0:
        logger.info(' {:20.20} | {:20.20} | {} missing rows'
                    .format(source_name, dataset_name, missing_rows))

    # Cut off the data outside of [start_from_user:end_from_user]
    # In order to make sure that the respective time period is covered in both
    # UTC and CE(S)T, we set the start in CE(S)T, but the end in UTC
    if start_from_user:
        start_from_user = (
            pytz.timezone('CET')
            .localize(datetime.combine(start_from_user, time()))
            .astimezone(pytz.timezone('UTC'))
            .replace(tzinfo=None))
    if end_from_user:
        end_from_user = (
            pytz.timezone('UTC')
            .localize(datetime.combine(end_from_user, time()))
            .replace(tzinfo=None)
            # Appropriate offset to include the end of period (23:45 for the
            # same day)
            + timedelta(days=1, minutes=-int(res_key[:2])))
    # Then cut off the data
    df = df.loc[start_from_user:end_from_user, :]

    # delete zeros before first/after last non-zero value in each column
    for col_name, col in df.iteritems():
        nan_for_zero = col.replace(0, np.nan)
        slicer = ((col.index <= nan_for_zero.first_valid_index()) |
                 (col.index >= nan_for_zero.last_valid_index()))
        col.loc[slicer] = np.nan

    return df


def update_progress(count, total, container):
    '''
    Display or updatesa console progress bar.

    Parameters
    ----------
    count : int
        Number of files that have been read so far
    total : int
        Total number of files
    container : str
        timerange of file currently bein read, also directory name of that file
    Returns
    ----------
    None

    '''

    barLength = 50  # Modify this to change the length of the progress bar
    status = 'reading'
    progress = count / total
    if isinstance(progress, int):
        progress = float(progress)
    if progress >= 1:
        progress = 1
        status = 'Done.\r\n'
        container = ''
    block = int(round(barLength * progress))
    text = '\rProgress: {0} {1}/{2} files {3} {4}'.format(
        '░' * block + '█' * (barLength - block), count, total, status, container)
    sys.stdout.write(text)
    sys.stdout.flush()

    return


def make_multiindex(
        df,
        colmap,
        headers,
        region=None,
        variable=None,
        attribute=None,
        url=None):
    '''
    Filter out unneeded columns from a DataFrame and create a pandas.MultiIndex
    contanining metadata for the parsed data.

    Parameters
    ----------
    df : pandas.DataFrame
        Data parsed from a file
    colmap : dict
        Maps the existing columnnameds to the values of the MultiIndex levels
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    region : string
        Value for header
    variable : string
        Value for header
    attribute : string
        Value for header
    url : string
        Value for header

    Returns
    ----------
    df : pandas.DataFrame
        The input DataFrame with unneeded columns removed and new MultiIndex
        contaning metadata for the parsed data
    '''

    # Drop any column not in colmap
    df = df[[key for key in colmap.keys() if key in df.columns]]

    # Create the MultiIndex
    cols = [tuple(colmap[col_name][level].format(region=region,
                                                 variable=variable,
                                                 attribute=attribute,
                                                 region_from_col=col_name)
        for level in headers) for col_name in df.columns]

#    for col_name in df.columns:
#        for level in headers:
#            if level in df.columns.names
#            if type(df.columns) == pd.core.indexes.multi.MultiIndex:

    df.columns = pd.MultiIndex.from_tuples(cols, names=headers)

    return df


def check_dst(index, autumn_expect=2, timezone='CET'):
    '''
    Count how many times the the daylight saving times (DST) transistion hours 
    in October and March appear in a DataFrame.index and compare against
    expected number, informing when they deviate. In reality, the DST-hours
    appear 0 times in March and 2 times in October.
    Note: When the expected number for October is 2, we don't need this function
    since df.index.tz_localize('Europe/Berlin', ambiguous='infer') throws an 
    exception if the actual number is 1

    Parameters
    ----------
    index : pandas.DatetimeIndex
        The index to be checked
    autumn_expect : int
        Number of times the DST hour is expected in autumn
    timezone : str
        A timezone name from pytz.all_timezones

    Returns
    ----------
    None

    '''
    transition_hour = {'CET': 2, 'WET': 1}
    dst_transitions_spring = [
        d.replace(hour=transition_hour[timezone])
        for d in pytz.timezone(timezone)._utc_transition_times
        if 2000 <= d.year <= datetime.today().year and d.month == 3]

    dst_transitions_autumn = [
        d.replace(hour=transition_hour[timezone])
        for d in pytz.timezone(timezone)._utc_transition_times
        if 2000 <= d.year <= datetime.today().year and d.month == 10]

    for d1, d2 in zip(dst_transitions_spring, dst_transitions_autumn):
        if d1 in index:
            logger.info('DST hours: spring: {} | {}'.format(
                d1.year, sum(index == d1)))
        if d2.replace(hour=0) in index and sum(index == d2) != autumn_expect:
            logger.info('DST hours: autumn: {} | {}'.format(
                d2.year, sum(index == d2)))

    return
