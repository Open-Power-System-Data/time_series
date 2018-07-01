"""
Open Power System Data

Timeseries Datapackage

read.py : read time series files

"""
import pytz
import yaml
import os
import sys
import numpy as np
import pandas as pd
import logging
import zipfile
import csv
import re
from datetime import datetime, date, time, timedelta
import xlrd

logger = logging.getLogger(__name__)
logger.setLevel('INFO')


def read_entso_e_transparency(
        areas, filepath, variable_name, url, headers, res_key, cols, stacked,
        unstacked, geo, append_headers, **kwargs):
    """
    Read a .csv file from ENTSO-E TRansparency into a DataFrame.
    Parameters
    ----------
    filepath : str
        Directory path of file to be read
    variable_name : str
        Name of variable, e.g. ``solar``
    url : str
        URL linking to the source website where this data comes from
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    res_key : str
        Resolution of the source data. Must be one of ['15min', '30min', 60min']
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
    geo: string
        The geographical concept (i.e. ``country`` or ``bidding zone`` for which
        data should be extracted.
        Records for other concepts (i.e. ``control areas``)) willl be ignored.
    append_headers: dict
        Map of header levels and values to append to Multiindex
    kwargs: dict
        placeholder for further named function arguments
    Returns
    ----------
    df: pandas.DataFrame
        The content of one file from PSE
    """

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
        usecols=['DateTime', *cols.keys()],
        # the column specifying the technology has a trailing space, which we
        # cut off
        converters={'ProductionType_Name': lambda x: x[:-1]},
    )

    if variable_name == 'Actual Generation per Production Type':
        # keep only renewables columns
        renewables = {
            'Solar': 'solar',
            'Wind Onshore': 'wind_onshore',
            'Wind Offshore': 'wind_offshore'
        }
        df_raw = df_raw[df_raw['ProductionType_Name'].isin(renewables.keys())]
        df_raw.replace({'ProductionType_Name': renewables}, inplace=True)

    if variable_name == 'Day Ahead Prices':
        # Omit polish price data reported in EUR (keeping PLN prices)
        # (Before 2017-03-02, the data is very messy)
        no_polish_euro = ~((df_raw['AreaName'] == 'PSE SA BZ') &
                           (df_raw.index < pd.to_datetime('2017-03-02 00:00:00')))
        df_raw = df_raw.loc[no_polish_euro]

    # keep only entries for selected geographic entities as specified in
    # areas.csv + select regions whith same temporal resolution
    time_and_place = areas[geo].loc[areas[res_key] == True].dropna()
    df_raw = df_raw.loc[df_raw['AreaName'].isin(time_and_place)]

    # based on the AreaName column, map the area names used throughout OPSD
    lookup = areas.set_index(geo)['area ID'].dropna()
    lookup = lookup[~lookup.index.duplicated()]
    df_raw['region'] = df_raw['AreaName'].map(lookup)
    df_raw.drop('AreaName', axis=1, inplace=True)

    # rename columns to comply with other data
    df_raw.rename(columns=cols, inplace=True)

    # juggle the index and columns
    df = df_raw
    df.set_index(stacked, append=True, inplace=True)
    # at this point, only the values we are intereseted in are are left as
    # columns
    df.columns.rename(unstacked, inplace=True)
    df = df.unstack(stacked)

    # keep only columns that have at least some nonzero values
    df = df.loc[:, (df > 0).any(axis=0)]

    # add source and url to the columns.
    # Note: pd.concat inserts new MultiIndex values infront of the old ones
    df = pd.concat([df],
                   keys=[tuple([*append_headers.values(), url])],
                   names=[*append_headers.keys(), 'web'],
                   axis='columns')

    # reorder and sort columns
    df = df.reorder_levels(headers, axis=1)
    df.sort_index(axis=1, inplace=True)

    # throw out obs with wrong timestamp
    #no_gaps = pd.DatetimeIndex(start=df.index[0],
    #                           end=df.index[-1],
    #                           freq=res_key)
    #df = df.reindex(index=no_gaps)

    return df


def read_pse(filepath, variable_name, url, headers):
    """
    Read a .csv file from PSE into a DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read
    variable_name : str
        Name of variable, e.g. ``solar``
    url : str
        URL linking to the source website where this data comes from
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe

    Returns
    ----------
    df: pandas.DataFrame
        The content of one file from PSE

    """

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
            'Time':
                lambda x: '2:00' if x == '2A' else str(int(x) - 1) + ':00'
        }
    )
    # Create a list of spring-daylight savings time (DST)-transitions
    dst_transitions_spring = [
        d.replace(hour=2)
        for d in pytz.timezone('Europe/Copenhagen')._utc_transition_times
        if d.year >= 2000 and d.month == 3]

    # Account for an error where an hour is jumped in the data, incrementing
    # the hour by one
    #time_int = df['Time'].str[:-3].astype(int)
    #if (time_int time_int.shift(1) - 1).
    #if (time_int == 24).any():
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
    df['timestamp'] = pd.to_datetime(
        df['Date'].astype(str) + ' ' + df['Time'])
    df.set_index('timestamp', inplace=True)

    # 'ambigous' refers to how the October dst-transition hour is handled.
    # 'infer' will attempt to infer dst-transition hours based on order.
    df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    colmap = {
        'Generation of Wind Farms': {
            'region': 'PL',
            'variable': 'wind_onshore',
            'attribute': 'generation_actual',
            'source': 'PSE',
            'web': url,
            'unit': 'MW'
        }
    }

    # Drop any column not in colmap
    df = df[list(colmap.keys())]

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level] for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_ceps(filepath, variable_name, url, headers):
    '''Read a file from CEPS into a DataFrame'''
    df = pd.read_csv(
        # pd.read_excel(io=filepath,
        #sheet_name='ČEPS report',
        filepath,
        sep=';',
        header=2,
        skiprows=None,
        index_col=0,
        usecols=[0, 1, 2]
    )

    df.index = pd.to_datetime(df.index.rename('timestamp'))

    df.index = df.index.tz_localize('Europe/Brussels', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    # Translate columns
    colmap = {
        'WPP [MW]': {
            'region': 'CZ',
            'variable': 'wind_onshore',
            'attribute': 'generation_actual',
            'source': 'CEPS',
            'web': url,
            'unit': 'MW'
        },
        'PVPP [MW]': {
            'region': 'CZ',
            'variable': 'solar',
            'attribute': 'generation_actual',
            'source': 'CEPS',
            'web': url,
            'unit': 'MW'
        }
    }

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level] for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_elia(filepath, variable_name, url, headers):
    '''Read a file from Elia into a DataFrame'''
    df = pd.read_excel(
        io=filepath,
        header=None,
        skiprows=4,
        index_col=0,
        usecols=None
    )

    colmap = {
        'Day-Ahead forecast [MW]': {
            'region': 'BE',
            'variable': variable,
            'attribute': 'generation_forecast',
            'source': 'Elia',
            'web': url,
            'unit': 'MW'
        },
        'Corrected Upscaled Measurement [MW]': {
            'region': 'BE',
            'variable': variable,
            'attribute': 'generation_actual',
            'source': 'Elia',
            'web': url,
            'unit': 'MW'
        },
        'Monitored Capacity [MWp]': {
            'region': 'BE',
            'variable': variable,
            'attribute': 'capacity',
            'source': 'Elia',
            'web': url,
            'unit': 'MW'
        }
    }

    # Drop any column not in colmap
    df = df[list(colmap.keys())]

    df.index = pd.to_datetime(df.index.rename('timestamp'))

    df.index = df.index.tz_localize('Europe/Brussels', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level] for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_energinet_dk(filepath, url, headers):
    '''Read a file from energinet.dk into a DataFrame'''
    df = pd.read_excel(
        io=filepath,
        header=2,  # the column headers are taken from 3rd row.
        # 2nd row also contains header info like in a multiindex,
        # i.e. wether the colums are price or generation data.
        # However, we will make our own columnnames below.
        # Row 3 is enough to unambigously identify the columns
        skiprows=None,
        index_col=None,
        usecols=None,  # None means: parse all columns
        thousands=','
    )

    # pandas on it's own authority sets first 2 columns as index
    # probably because the column's names are in merged cells
    df.index.rename(['date', 'hour'], inplace=True)
    df.reset_index(inplace=True)
    df['timestamp'] = pd.to_datetime(
        df['date'].astype(str) + ' ' +
        (df['hour'] - 1).astype(str) + ':00')
    df.set_index('timestamp', inplace=True)

    # Create a list of spring-daylight savings time (DST)-transitions
    dst_transitions_spring = [
        d.replace(hour=2)
        for d in pytz.timezone('Europe/Copenhagen')._utc_transition_times
        if d.year >= 2000 and d.month == 3]

    # Drop 3rd hour for (spring) DST-transition from df.
    df = df[~df.index.isin(dst_transitions_spring)]

    dst_arr = np.ones(len(df.index), dtype=bool)
    df.index = df.index.tz_localize('Europe/Copenhagen', ambiguous=dst_arr)
    df.index = df.index.tz_convert(None)

    source = 'Energinet.dk'
    colmap = {
        'DK-Vest': {
            'variable': 'price',
            'region': 'DK_1',
            'attribute': 'day_ahead',
            'source': source,
            'web': url,
            'unit': 'EUR'
        },
        'DK-Øst': {
            'variable': 'price',
            'region': 'DK_2',
            'attribute': 'day_ahead',
            'source': source,
            'web': url,
            'unit': 'EUR'
        },
        'Norge': {
            'variable': 'price',
            'region': 'NO',
            'attribute': 'day_ahead',
            'source': source,
            'web': url,
            'unit': 'EUR'
        },
        'Sverige (SE)': {
            'variable': 'price',
            'region': 'SE',
            'attribute': 'day_ahead',
            'source': source,
            'web': url,
            'unit': 'EUR'
        },
        'Sverige (SE3)': {
            'variable': 'price',
            'region': 'SE_3',
            'attribute': 'day_ahead',
            'source': source,
            'web': url,
            'unit': 'EUR'
        },
        'Sverige (SE4)': {
            'variable': 'price',
            'region': 'SE_4',
            'attribute': 'day_ahead',
            'source': source,
            'web': url,
            'unit': 'EUR'
        },
        'DE European Power Exchange': {
            'variable': 'price',
            'region': 'DE',
            'attribute': 'day_ahead',
            'source': source,
            'web': url,
            'unit': 'EUR'
        },
        'DK-Vest: Vindproduktion': {
            'variable': 'wind',
            'region': 'DK_1',
            'attribute': 'generation_actual',
            'source': source,
            'web': url,
            'unit': 'MW'
        },
        'DK-Vest: Solcelle produktion (estimeret)': {
            'variable': 'solar',
            'region': 'DK_1',
            'attribute': 'generation_actual',
            'source': source,
            'web': url,
            'unit': 'MW'
        },
        'DK-Øst: Vindproduktion': {
            'variable': 'wind',
            'region': 'DK_2',
            'attribute': 'generation_actual',
            'source': source,
            'web': url,
            'unit': 'MW'
        },
        'DK-Øst: Solcelle produktion (estimeret)': {
            'variable': 'solar',
            'region': 'DK_2',
            'attribute': 'generation_actual',
            'source': source,
            'web': url,
            'unit': 'MW'
        },
        'DK: Vindproduktion (onshore)': {
            'variable': 'wind_onshore',
            'region': 'DK',
            'attribute': 'generation_actual',
            'source': source,
            'web': url,
            'unit': 'MW'
        },
        'DK: Vindproduktion (offshore)': {
            'variable': 'wind_offshore',
            'region': 'DK',
            'attribute': 'generation_actual',
            'source': source,
            'web': url,
            'unit': 'MW'
        },
    }

    # Drop any column not in colmap
    colmap = {k: v for k, v in colmap.items() if k in df.columns}
    df = df[list(colmap.keys())]

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level] for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_entso_e_statistics(filepath, url, headers):
    '''Read a file from ENTSO-E into a DataFrame'''
    df = pd.read_excel(
        io=filepath,
        header=10,
        usecols='A, B, D, E, H, J, M:AQ'
    )

    df.drop(index=0, inplace=True)
    renamer = {'Date Time (CET/CEST)': 'date', df.columns[1]: 'time'}
    df.rename(columns=renamer, inplace=True)
    df['time'] = df['time'].str[:5]
    df['date'] = df['date'].fillna(method='ffill').dt.strftime('%Y-%m-%d')

    # fixes for individual rows
    df.loc[df['date'] == '2017-12-31', 'time'] = [
        "{:02d}:00".format(x) for x in range(0, 24)]
    df.loc[df['date'] == '2018-03-25', 'time'] = [
        "{:02d}:00".format(x) for x in range(-1, 24) if not x == 2]
    df = df[df['time'] != '-1:00']

    df.index = pd.to_datetime(df['date'] + ' ' + df['time'])
    df.drop(columns=['date', 'time'], inplace=True)
    df.rename(columns=lambda x: x[:2], inplace=True)

    df.index = df.index.tz_localize('Europe/Brussels', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    colmap = {
        'variable': 'load',
        'region': '{country}',
        'attribute': 'entsoe_power_statistics',
        'source': 'ENTSO-E Data Portal and Power Statistics',
        'web': url,
        'unit': 'MW'
    }

    # Create the MultiIndex
    tuples = [tuple(colmap[level].format(country=col)
                    for level in headers) for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_entso_e_portal(filepath, url, headers):
    '''Read a file from ENTSO-E into a DataFrame'''
    df = pd.read_excel(
        io=xlrd.open_workbook(filepath, logfile=open(os.devnull, 'w')),
        header=9,  # 0 indexed, so the column names are actually in the 10th row
        skiprows=None,
        # create MultiIndex from first 2 columns ['Country', 'Day']
        index_col=[0, 1],
        usecols=None,  # None means: parse all columns
        na_values=['n.a.'],
        engine='xlrd'
    )

    df.columns.names = ['raw_hour']

    # The original data has days and countries in the rows and hours in the
    # columns.  This rearranges the table, mapping hours on the rows and
    # countries on the columns.
    df = df.stack(level='raw_hour').unstack(level='Country').reset_index()

    # Format of the raw_hour-column is normally is 01:00:00, 02:00:00 etc.
    # during the year, but 3A:00:00, 3B:00:00 for the (possibely
    # DST-transgressing) 3rd hour of every day in October, we truncate the
    # hours column after 2 characters and replace letters which are there to
    # indicate the order during fall DST-transition.
    df['hour'] = df['raw_hour'].str[:2].str.replace(
        'A', '').str.replace('B', '')
    # Hours are indexed 1-24 by ENTSO-E, but pandas requires 0-23, so we
    # deduct 1, i.e. the 3rd hour will be indicated by "2:00" rather than
    # "3:00"
    df['hour'] = (df['hour'].astype(int) - 1).astype(str)

    df['timestamp'] = pd.to_datetime(df['Day'] + ' ' + df['hour'] + ':00')
    df.set_index('timestamp', inplace=True)

    # Create a list of daylight savings time (DST)-transitions
    dst_transitions = [
        d.replace(hour=2)
        for d in pytz.timezone('Europe/Berlin')._utc_transition_times
        if d.year >= 2000]

    # Drop 2nd occurence of 3rd hour appearing in October file
    # except for the day of the actual autumn DST-transition.
    df = df[~((df['raw_hour'] == '3B:00:00') & ~
              (df.index.isin(dst_transitions)))]

    # Drop 3rd hour for (spring) DST-transition. October data
    # is unaffected the format is 3A:00:00/3B:00:00.
    df = df[~((df['raw_hour'] == '03:00:00') &
              (df.index.isin(dst_transitions)))]

    df.drop(['Day', 'hour', 'raw_hour'], axis=1, inplace=True)
    df.index = df.index.tz_localize('Europe/Brussels', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    renamer = {'DK_W': 'DK_1', 'UA_W': 'UA_west', 'NI': 'GB_NIR'}
    df.rename(columns=renamer, inplace=True)

    colmap = {
        'variable': 'load',
        'region': '{country}',
        'attribute': 'entsoe_power_statistics',
        'source': 'ENTSO-E Data Portal and Power Statistics',
        'web': url,
        'unit': 'MW'
    }

    # Create the MultiIndex
    tuples = [tuple(colmap[level].format(country=col)
                    for level in headers) for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_hertz(filepath, variable_name, url, headers):
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

    # Until 2006, and in 2015 (except for wind_generation_pre-offshore),
    # during the fall dst-transistion, only the
    # wintertime hour (marked by a B in the data) is reported, the summertime
    # hour, (marked by an A) is missing in the data.
    # dst_arr is a boolean array consisting only of "False" entries, telling
    # python to treat the hour from 2:00 to 2:59 as wintertime.
    if (pd.to_datetime(df.index.values[0]).year not in [2005, 2006, 2015] or
            (variable_name == 'wind generation_actual pre-offshore' and
             pd.to_datetime(df.index.values[0]).year == 2015)):
        df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    else:
        dst_arr = np.zeros(len(df.index), dtype=bool)
        df.index = df.index.tz_localize('Europe/Berlin', ambiguous=dst_arr)

    df.index = df.index.tz_convert(None)

    tech, attribute = variable_name.split(' ')[:2]

    colmap = {
        'MW': {
            'variable': '{tech}',
            'region': 'DE_50hertz',
            'attribute': '{attribute}',
            'source': '50Hertz',
            'web': url,
            'unit': 'MW'
        },
        'Onshore MW': {
            'variable': 'wind_onshore',
            'region': 'DE_50hertz',
            'attribute': '{attribute}',
            'source': '50Hertz',
            'web': url,
            'unit': 'MW'
        },
        'Offshore MW': {
            'variable': 'wind_offshore',
            'region': 'DE_50hertz',
            'attribute': '{attribute}',
            'source': '50Hertz',
            'web': url,
            'unit': 'MW'
        }
    }
    # Since 2016, wind data has an aditional column for offshore.
    # Baltic 1 has been producing since 2011-05-02 and Baltic2 since
    # early 2015 (source: Wikipedia) so it is probably not correct that
    # 50Hertz-Wind data pre-2016 is only onshore. Maybe we can ask at
    # 50Hertz directly.

    # Drop any column not in colmap
    df = df[[key for key in colmap.keys() if key in df.columns]]

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level].format(tech=tech, attribute=attribute)
                    for level in headers) for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_amprion(filepath, variable_name, url, headers):
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

    index1 = df.index[df.index.year <= 2009]
    index1 = index1.tz_localize('Europe/Berlin', ambiguous='infer')

    # In the years after 2009, during the fall dst-transistion, only the
    # summertime hour is reported, the wintertime hour is missing in the data.
    # dst_arr is a boolean array consisting only of "True" entries, telling
    # python to treat the hour from 2:00 to 2:59 as summertime.
    index2 = df.index[df.index.year > 2009]
    dst_arr = np.ones(len(index2), dtype=bool)
    index2 = index2.tz_localize('Europe/Berlin', ambiguous=dst_arr)
    df.index = index1.append(index2)
    df.index = df.index.tz_convert(None)

    tech = variable_name
    colmap = {
        '8:00 Uhr Prognose [MW]': {
            'variable': '{tech}',
            'region': 'DE_amprion',
            'attribute': 'generation_forecast',
            'source': 'Amprion',
            'web': url,
            'unit': 'MW'
        },
        'Online Hochrechnung [MW]': {
            'variable': '{tech}',
            'region': 'DE_amprion',
            'attribute': 'generation_actual',
            'source': 'Amprion',
            'web': url,
            'unit': 'MW'
        }
    }

    # Drop any column not in colmap
    df = df[list(colmap.keys())]

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level].format(tech=tech) for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_tennet(filepath, variable_name, url, headers):
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

    renamer = {'Datum': 'date', 'Position': 'pos'}
    df = df.rename(columns=renamer)

    df['date'].fillna(method='ffill', limit=100, inplace=True)

    # Check the rows for irregularities
    for i in range(len(df.index)):
        # On the day in March when summertime begins, shift the data forward by
        # 1 hour, beginning with the 9th quarter-hour, so the index runs again
        # up to 96
        if (df['pos'][i] == 92 and ((i == len(df.index) - 1) or
                                    (df['pos'][i + 1] == 1))):
            slicer = df[(df['date'] == df['date'][i]) & (df['pos'] >= 9)].index
            df.loc[slicer, 'pos'] = df['pos'] + 4

        elif df['pos'][i] > 96:  # True when summertime ends in October
            logger.debug('%s th quarter-hour at %s, position %s',
                         df['pos'][i], df.ix[i, 'date'], (i))

            # Instead of having the quarter-hours' index run up to 100, we want
            # to have it set back by 1 hour beginning from the 13th
            # quarter-hour, ending at 96
            if df['pos'][i] == 100 and not (df['pos'] == 101).any():
                slicer = df[(df['date'] == df['date'][i])
                            & (df['pos'] >= 13)].index
                df.loc[slicer, 'pos'] = df['pos'] - 4

    # Compute timestamp from position and generate datetime-index
    df['hour'] = (np.trunc((df['pos'] - 1) / 4)).astype(int).astype(str)
    df['minute'] = (((df['pos'] - 1) % 4) * 15).astype(int).astype(str)
    df['timestamp'] = pd.to_datetime(df['date'] + ' ' + df['hour'] + ':' +
                                     df['minute'], dayfirst=True)
    df.set_index('timestamp', inplace=True)

    df.drop(['pos', 'date', 'hour', 'minute'], axis=1, inplace=True)

    df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    tech = variable_name
    colmap = {
        'prognostiziert [MW]': {
            'variable': '{tech}',
            'region': 'DE_tennet',
            'attribute': 'generation_forecast',
            'source': 'TenneT',
            'web': url,
            'unit': 'MW'
        },
        'tatsächlich [MW]': {
            'variable': '{tech}',
            'region': 'DE_tennet',
            'attribute': 'generation_actual',
            'source': 'TenneT',
            'web': url,
            'unit': 'MW'
        },
        'Anteil Offshore [MW]': {
            'variable': 'wind_offshore',
            'region': 'DE_tennet',
            'attribute': 'generation_actual',
            'source': 'TenneT',
            'web': url,
            'unit': 'MW'
        }
    }

    # Drop any column not in colmap
    df = df[[key for key in colmap.keys() if key in df.columns]]

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level].format(tech=tech) for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_transnetbw(filepath, variable_name, url, headers):
    '''Read a file from TransnetBW into a DataFrame'''
    df = pd.read_csv(
        filepath,
        sep=';',
        header=0,
        index_col='timestamp',
        parse_dates={'timestamp': ['Datum bis', 'Uhrzeit bis']},
        date_parser=None,
        dayfirst=True,
        decimal=',',
        thousands=None,
        converters=None,
    )

    # DST-transistion is conducted 2 hours too late in the data
    # (hour 4:00-5:00 is repeated instead of 2:00-3:00)
    if df.index[0].date().month == 10:
        df.index = pd.DatetimeIndex(start=df.index[0],
                                    end=df.index[-1],
                                    freq='15min',
                                    tz=pytz.timezone('Europe/Berlin'))
    else:
        df.index = df.index.tz_localize('Europe/Berlin')
    df.index = df.index.tz_convert(None)

    # The 2nd column represents the start and the 4th the end of the respective
    # period. The former has some errors, so we use the latter to construct the
    # index and shift the data back by 1 period.
    df = df.shift(periods=-1, freq='15min', axis='index')

    tech = variable_name
    colmap = {
        'Prognose (MW)': {
            'variable': '{tech}',
            'region': 'DE_transnetbw',
            'attribute': 'generation_forecast',
            'source': 'TransnetBW',
            'web': url,
            'unit': 'MW'
        },
        'Ist-Wert (MW)': {
            'variable': '{tech}',
            'region': 'DE_transnetbw',
            'attribute': 'generation_actual',
            'source': 'TransnetBW',
            'web': url,
            'unit': 'MW'
        }
    }

    # Drop any column not in colmap
    df = df[list(colmap.keys())]

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level].format(tech=tech) for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_opsd(filepath, url, headers):
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

    # The capacities data only has one entry per day, which pandas
    # interprets as 00:00h. We will broadcast the dayly data for
    # all quarter-hours of the day until the next given data point.
    # For this, we we expand the index so it reaches to 23:59 of
    # the last day, not only 00:00.
    last = pd.to_datetime([df.index[-1]]) + timedelta(days=1, minutes=59)
    until_last = df.index.append(last).rename('timestamp')
    df = df.reindex(index=until_last, method='ffill')
    df.index = df.index.tz_localize('Europe/Berlin')
    df.index = df.index.tz_convert(None)
    df = df.resample('15min').ffill().round(0)

    colmap = {
        'Solar': {
            'variable': 'solar',
            'region': 'DE',
            'attribute': 'capacity',
            'source': 'own calculation based on BNetzA and netztransparenz.de',
            'web': url,
            'unit': 'MW'
        },
        'Onshore': {
            'variable': 'wind_onshore',
            'region': 'DE',
            'attribute': 'capacity',
            'source': 'own calculation based on BNetzA and netztransparenz.de',
            'web': url,
            'unit': 'MW'
        },
        'Offshore': {
            'variable': 'wind_offshore',
            'region': 'DE',
            'attribute': 'capacity',
            'source': 'own calculation based on BNetzA and netztransparenz.de',
            'web': url,
            'unit': 'MW'
        }
    }

    # Drop any column not in colmap
    df = df[list(colmap.keys())]

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level] for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_svenska_kraftnaet(filePath, variable_name, url, headers):
    '''Read a file from Svenska Kraftnät into a DataFrame'''
    if variable_name in ['wind_solar_1', 'wind_solar_2']:
        skip = 4
        cols = [0, 1, 3]
        colnames = ['date', 'hour', 'wind']
    else:
        if variable_name == 'wind_solar_4':
            skip = 5
        else:
            skip = 7
        cols = [0, 2, 8]
        colnames = ['timestamp', 'wind', 'solar']

    df = pd.read_excel(
        io=filePath,
        # read the last sheet (in some years,
        # there are hidden sheets that would cause errors)
        sheet_name=-1,
        header=None,
        skiprows=skip,
        index_col=None,
        usecols=cols
    )

    # renamer = {'Tid': 'timestamp', 'DATUM': 'date', 'TID': 'hour'}
    df.columns = colnames

    if variable_name in ['wind_solar_1', 'wind_solar_2']:
        # in 2009 there is a row below the table for the sums that we don't
        # want to read in
        df = df[df['date'].notnull()]
        df['timestamp'] = pd.to_datetime(
            df['date'].astype(int).astype(str) + ' ' +
            df['hour'].astype(int).astype(str).str.replace('00', '') + ':00',
            dayfirst=False,
            infer_datetime_format=True)
        df.drop(['date', 'hour'], axis=1, inplace=True)
    else:
        # in 2011 there is a row below the table for the sums that we don't
        # want to read in
        df = df[((df['timestamp'].notnull()) &
                 (df['timestamp'].astype(str) != 'Tot summa GWh'))]
        df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=True)

    df.set_index('timestamp', inplace=True)
    # The timestamp ("Tid" in the original) gives the time without
    # daylight savings time adjustments (normaltid). To convert to UTC,
    # one hour has to be deducted
    df.index = df.index + pd.offsets.Hour(-1)

    colmap = {
        'wind': {
            'variable': 'wind',
            'region': 'SE',
            'attribute': 'generation_actual',
            'source': 'Svenska Kraftnaet',
            'web': url,
            'unit': 'MW'
        },
        'solar': {
            'variable': 'solar',
            'region': 'SE',
            'attribute': 'generation_actual',
            'source': 'Svenska Kraftnaet',
            'web': url,
            'unit': 'MW'
        }
    }

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level] for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_apg(filepath, url, headers):
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
        # during the year, but 3A:00:00, 3B:00:00 for the (possibely
        # DST-transgressing) 3rd hour of every day in October, we truncate the
        # hours column after 2 characters and replace letters which are there to
        # indicate the order during fall DST-transition.
        converters={'Von': lambda x: str(x).replace('A', '').replace('B', '')}
    )

    df.index = df.index.tz_localize('Europe/Vienna', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    colmap = {
        'Wind [MW]': {
            'variable': 'wind_onshore',
            'region': 'AT',
            'attribute': 'generation_actual',
            'source': 'APG',
            'web': url,
            'unit': 'MW'
        },
        'Solar [MW]': {
            'variable': 'solar',
            'region': 'AT',
            'attribute': 'generation_actual',
            'source': 'APG',
            'web': url,
            'unit': 'MW'
        },
        'Wind  [MW]': {
            'variable': 'wind_onshore',
            'region': 'AT',
            'attribute': 'generation_actual',
            'source': 'APG',
            'web': url,
            'unit': 'MW'
        },
        'Solar  [MW]': {
            'variable': 'solar',
            'region': 'AT',
            'attribute': 'generation_actual',
            'source': 'APG',
            'web': url,
            'unit': 'MW'
        }
    }

    # Drop any column not in colmap
    df = df[[key for key in colmap.keys() if key in df.columns]]

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level] for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_rte(filepath, variable_name, url, headers):
    '''Read a file from RTE into a DataFrame'''

    # pandas.read_csv infers the table dimensions from the header row.
    # Since the first row uses only one column, it needs to be read separately
    # in order not to mess up the DataFrame
    df1 = pd.read_csv(
        filepath,
        sep='\t',
        encoding='cp1252',
        compression='zip',
        nrows=1,
        header=None
    )
    df2 = pd.read_csv(
        filepath,
        sep='\t',
        encoding='cp1252',
        compression='zip',
        skiprows=[0],
        header=None,
        usecols=list(range(0, 13))
    )

    # Glue the DataFrames together
    df = pd.concat([df1, df2], ignore_index=True)

    # set column names
    df = df.rename(columns=df.iloc[1])

    # strip the cells with dates of any other text
    df['Heures'] = df['Heures'].str.lstrip('Données de réalisation du ')

    # fill an extra column with corresponding dates.
    df['Dates'] = np.nan
    slicer = df['Heures'].str.match('\d{2}/\d{2}/\d{4}')
    df['Dates'] = df['Heures'].loc[slicer]
    df['Dates'].fillna(method='ffill', axis=0, inplace=True, limit=25)

    # drop all rows not containing data (no time-format in first column)
    df = df.loc[df['Heures'].str.len() == 11]

    # drop daylight saving time hours (no data there)
    df = df.loc[(df['Éolien terrestre'] != '*') & (df['Solaire'] != '*')]

    # just display beginning of hours
    df['Heures'] = df['Heures'].str[:5]

    # construct full date to later use as index
    df['timestamp'] = df['Dates'] + ' ' + df['Heures']
    df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=True,)

    # drop autumn dst hours as they contain inconsistent data (none or copy of
    # hour before)
    dst_transitions_autumn = [
        d.replace(hour=2)
        for d in pytz.timezone('Europe/Paris')._utc_transition_times
        if d.year >= 2000 and d.month == 10]
    df = df.loc[~df['timestamp'].isin(dst_transitions_autumn)]
    df.set_index(df['timestamp'], inplace=True)

    # Transfer to UTC
    df.index = df.index.tz_localize('Europe/Paris')
    df.index = df.index.tz_convert(None)

    colmap = {
        'Éolien terrestre': {
            'variable': 'wind_onshore',
            'region': 'FR',
            'attribute': 'generation_actual',
            'source': 'RTE',
            'web': url,
            'unit': 'MW'
        },
        'Solaire': {
            'variable': 'solar',
            'region': 'FR',
            'attribute': 'generation_actual',
            'source': 'RTE',
            'web': url,
            'unit': 'MW'
        }
    }

    # Drop any column not in colmap
    df = df[[key for key in colmap.keys() if key in df.columns]]

    # Create the MultiIndex
    tuples = [tuple(colmap[col][level] for level in headers)
              for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read(data_path, areas, source_name, variable_name, res_key,
         headers, param_dict, start_from_user=None, end_from_user=None):
    """
    For the sources specified in the sources.yml file, pass each downloaded
    file to the correct read function.

    Parameters
    ----------
    source_name : str
        Name of source to read files from
    variable_name : str
        Indicator for subset of data available together in the same files
    param_dict : dict
        Dictionary of further parameters, i.e. the URL of the Source to be
        placed in the column-MultiIndex
    res_key : str
        Resolution of the source data. Must be one of ['15min', '30min', 60min']
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe
    data_path : str, default: 'original_data'
        Base download directory in which to save all downloaded files
    start_from_user : datetime.date, default None
        Start of period for which to read the data
    end_from_user : datetime.date, default None
        End of period for which to read the data

    Returns
    ----------
    data_set: pandas.DataFrame
        A DataFrame containing the combined data for variable_name 

    """
    data_set = pd.DataFrame()

    variable_dir = os.path.join(data_path, source_name, variable_name)

    logger.info('reading %s - %s', source_name, variable_name)

    files_existing = sum([len(files) for r, d, files in os.walk(variable_dir)])
    files_success = 0

    # Check if there are folders for variable_name
    if not os.path.exists(variable_dir):
        logger.warning('folder not found for %s, %s',
                       source_name, variable_name)
        return data_set

    # For each file downloaded for that variable
    for container in sorted(os.listdir(variable_dir)):
        # Skip this file if period covered excluded by user
        if start_from_user:
            # start lies after file end => filecontent is too old
            if start_from_user > yaml.load(container.split('_')[1]):
                continue  # go to next container

        if end_from_user:
            # end lies before file start => filecontent is too recent
            if end_from_user < yaml.load(container.split('_')[0]) - timedelta(days=1):
                continue  # go to next container

        files = os.listdir(os.path.join(variable_dir, container))

        # Check if there is only one file per folder
        if len(files) == 0:
            logger.warning('found no file in %s %s %s',
                           source_name, variable_name, container)
            continue

        elif len(files) > 1:
            logger.warning('found more than one file in %s %s %s',
                           source_name, variable_name, container)
            continue

        filepath = os.path.join(variable_dir, container, files[0])

        # Check if file is not empty
        if os.path.getsize(filepath) < 128:
            logger.warning('%s \n file is smaller than 128 Byte. It is probably'
                           ' empty and will thus be skipped from reading',
                           filepath)
        else:
            logger.debug('reading data:\n\t '
                         'Source:   %s\n\t '
                         'Variable: %s\n\t '
                         'Filename: %s',
                         source_name, variable_name, files[0])

            update_progress(files_success, files_existing)

            url = param_dict['web']

            if source_name == 'OPSD':
                data_to_add = read_opsd(filepath, url, headers)
            elif source_name == 'CEPS':
                data_to_add = read_ceps(filepath, variable_name, url, headers)
            elif source_name == 'ENTSO-E Transparency FTP':
                data_to_add = read_entso_e_transparency(
                    areas, filepath, variable_name, url, headers, res_key,
                    **param_dict)
            elif source_name == 'ENTSO-E Data Portal':
                data_to_add = read_entso_e_portal(filepath, url, headers)
            elif source_name == 'ENTSO-E Power Statistics':
                data_to_add = read_entso_e_statistics(filepath, url, headers)
            elif source_name == 'Energinet.dk':
                data_to_add = read_energinet_dk(filepath, url, headers)
            elif source_name == 'Elia':
                data_to_add = read_elia(filepath, variable_name, url, headers)
            elif source_name == 'PSE':
                data_to_add = read_pse(filepath, variable_name, url, headers)
            elif source_name == 'RTE':
                data_to_add = read_rte(filepath, variable_name, url, headers)
            elif source_name == 'Svenska Kraftnaet':
                data_to_add = read_svenska_kraftnaet(
                    filepath, variable_name, url, headers)
            elif source_name == '50Hertz':
                data_to_add = read_hertz(filepath, variable_name, url, headers)
            elif source_name == 'Amprion':
                data_to_add = read_amprion(
                    filepath, variable_name, url, headers)
            elif source_name == 'TenneT':
                data_to_add = read_tennet(
                    filepath, variable_name, url, headers)
            elif source_name == 'TransnetBW':
                data_to_add = read_transnetbw(
                    filepath, variable_name, url, headers)
            elif source_name == 'APG':
                data_to_add = read_apg(filepath, url, headers)

            if data_set.empty:
                data_set = data_to_add
            else:
                data_set = data_set.combine_first(data_to_add)

            files_success += 1
            update_progress(files_success, files_existing)

    if data_set.empty:
        logger.warning('returned empty DataFrame for %s, %s',
                       source_name, variable_name)
        return data_set

    # Reindex with a new index that is sure to be continous in order to later
    # expose gaps in the data.
    no_gaps = pd.DatetimeIndex(start=data_set.index[0],
                               end=data_set.index[-1],
                               freq=res_key)
    data_set = data_set.reindex(index=no_gaps)

    # Cut off the data outside of [start_from_user:end_from_user]
    # In order to make sure that the respective time period is covered in both
    # UTC and CE(S)T, we set the start in CE(S)T, but the end in UTC
    if start_from_user:
        start_from_user = (
            pytz.timezone('Europe/Brussels')
            .localize(datetime.combine(start_from_user, time()))
            .astimezone(pytz.timezone('UTC')))
    if end_from_user:
        end_from_user = (
            pytz.timezone('UTC')
            .localize(datetime.combine(end_from_user, time()))
            # Appropriate offset to include the end of period (23:45 for the
            # same day)
            + timedelta(days=1, minutes=-int(res_key[:2])))
    # Then cut off the data_set
    data_set = data_set.loc[start_from_user:end_from_user, :]

    return data_set


def update_progress(count, total):
    '''
    Display or updates a console progress bar.

    Parameters
    ----------
    count : int
        number of files that have been read so far
    total : int
        total number aif files

    Returns
    ----------
    None

    '''

    barLength = 50  # Modify this to change the length of the progress bar
    status = ""
    progress = count / total
    if isinstance(progress, int):
        progress = float(progress)
    if progress >= 1:
        progress = 1
        status = "Done...\r\n"
    block = int(round(barLength * progress))
    text = "\rProgress: {0} {1}/{2} files {3}".format(
        "░" * block + "█" * (barLength - block), count, total, status)
    sys.stdout.write(text)
    sys.stdout.flush()

    return
