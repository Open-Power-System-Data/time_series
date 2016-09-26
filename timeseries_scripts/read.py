"""
Open Power System Data

Timeseries Datapackage

read.py : read time series files

"""
import pytz
import yaml
import os
import numpy as np
import pandas as pd
import logging
import zipfile

logger = logging.getLogger('log')
logger.setLevel('INFO')

def read_rte(filepath, variable_name, url, headers):
    pass
    return

def read_pse(filepath, variable_name, url, headers):
    """
    Read a .csv file with wind or solar power timeseries data from 
    TransnetBW into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    variable_name : str
        Name of variable, e.g. ``solar`
    url : str
        URL linking to the source website where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='cp1250',
        header=0,
        index_col='timestamp',
        names=None, #['date', 'time', 'forecast', 'generation'],
        parse_dates={'timestamp': ['Data', 'Godzina']},
        date_parser=None,         
        dayfirst=False,
        decimal=',',
        thousands=None,
        converters={'Godzina': lambda x: str(int(x[:2]) - 1) + ':00'},
        usecols=None, 
        infer_datetime_format=True
    )
    
    # 'ambigous' refers to how the October dst-transition hour is handled.  
    # ‘infer’ will attempt to infer dst-transition hours based on order.
    df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    df.index = df.index.tz_convert(None)
    
    # Translate columns
    colmap = {'Sumaryczna generacja źródeł wiatrowych': ('wind', 'PL', 'generation', 'PSE', url)}
        
    # Create the MultiIndex. 
    tuples = [colmap[col] for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    
    return df

def read_ceps(filepath, variable_name, url, headers):
    df = pd.read_excel(
        io=filepath,
        header=2,
        skiprows=None,
        index_col=0,
        parse_cols=[0, 1, 2]
    )
    
    df.index = pd.to_datetime(df.index.rename('timestamp'))
    
    df.index = df.index.tz_localize('Europe/Brussels', ambiguous='infer')
    df.index = df.index.tz_convert(None)
    
    # Translate columns
    colmap = {'WPP [MW]': ('wind-onshore', 'CZ', 'generation', 'CEPS', url),
              'PVPP [MW]': ('solar', 'CZ', 'generation', 'CEPS', url),}
        
    # Create the MultiIndex. 
    tuples = [colmap[col] for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    
    return df

def read_elia(filepath, variable_name, url, headers):
    """
    Read a .csv file with wind or solar power timeseries data from 
    Elia into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    variable_name : str
        Name of variable, e.g. ``solar`
    url : str
        URL linking to the source website where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
    df = pd.read_excel(
        io=filepath,
        header=None,
        skiprows=4,
        index_col=0,
        parse_cols=[0, 2, 4, 5]
    )

    df.columns = ['forecast', 'generation', 'capacity']
    
    df.index = pd.to_datetime(df.index.rename('timestamp'))
    
    df.index = df.index.tz_localize('Europe/Brussels', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    # Create the MultiIndex
    tuples = [(variable_name, 'BE', attribute, 'Elia', url)
              for attribute in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    
    return df


def read_energinet_dk(filepath, url, headers):
    """
    Read a .csv file with wind/solar power timeseries and price data from 
    TransnetBW into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    web : str
        URL linking to the source urlsite where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
    df = pd.read_excel(
        io=filepath,
        header=2, # the column headers are taken from 3rd row. 
        # 2nd row also contains header info like in a multiindex, 
        # i.e. wether the colums are price or generation data. 
        # However, we will make our own columnnames below. 
        # Row 3 is enough to unambigously identify the columns 
        skiprows=None,
        index_col=None, 
        parse_cols=None # None means: parse all columns
    )
    
    df.index.rename(['date', 'hour'], inplace=True)  # JONATHAN Wieso hier eine Liste?
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
        'DK-West': 
            ('price', 'DK-West', 'Elspot', source, url),
        'DK-East': 
            ('price', 'DK-East', 'Elspot', source, url),
        'Norway': 
            ('price', 'NO', 'Elspot', source, url),
        'Sweden (SE)': 
            ('price', 'SE', 'Elspot', source, url),
        'Sweden (SE3)': 
            ('price', 'SE-3', 'Elspot', source, url),
        'Sweden (SE4)': 
            ('price', 'SE-4', 'Elspot', source, url),
        'DE European Power Exchange': 
            ('price', 'DE', 'EPEX', source, url),
        'DK-West: Wind power production': 
            ('wind-total', 'DK-West', 'generation', source, url),
        'DK-West: Solar cell production (estimated)': 
            ('solar', 'DK-West', 'generation', source, url),
        'DK-East: Wind power production': 
            ('wind-total', 'DK-East', 'generation', source, url),
        'DK-East: Solar cell production (estimated)': 
            ('solar', 'DK-East', 'generation', source, url),
        'DK: Wind power production (onshore)': 
            ('wind-onshore', 'DK', 'generation', source, url),
        'DK: Wind power production (offshore)': 
            ('wind-offshore', 'DK', 'generation', source, url)
    }
    
    # Drop any column not in colmap
    df = df[list(colmap.keys())]
    
    # Create the MultiIndex. 
    tuples = [colmap[col] for col in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    # Drop any other columns that might have been included in download
    df.drop(['drop_me'], axis=1, level=2, inplace=True)
        
    return df


def read_entso(filepath, url, headers):
    """
    Read a .xls file with hourly load data from the ENTSO Data Portal
    into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    url : str
        URL linking to the source website where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
    df = pd.read_excel(
        io=filepath,
        header=9, # 0 indexed, so the column names are actually in the 10th row
        skiprows=None,
        index_col=[0, 1], # create MultiIndex from first 2 columns ['Country', 'Day']
        parse_cols=None, # None means: parse all columns
        na_values=['n.a.']
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
    df['hour'] = df['raw_hour'].str[:2].str.replace('A','').str.replace('B','')
    # Hours are indexed 1-24 by ENTSO-E, but pandas requires 0-23, so we deduct 1,
    # i.e. the 3rd hour will be indicated by "2:00" rather than "3:00"
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
    df = df[~((df['raw_hour'] == '3B:00:00') & ~(df.index.isin(dst_transitions)))]
    
    # Drop 3rd hour for (spring) DST-transition. October data 
    # is unaffected the format is 3A:00:00/3B:00:00.  
    df = df[~((df['raw_hour'] == '03:00:00') & (df.index.isin(dst_transitions)))]
    
    df.drop(['Day', 'hour', 'raw_hour'], axis=1, inplace=True)
    df.index = df.index.tz_localize('Europe/Brussels', ambiguous='infer')
    df.index = df.index.tz_convert(None)
    
    df.rename(columns={'DK_W': 'DKw', 'UA_W': 'UAw'}, inplace=True)
    
    # Create the MultiIndex.  
    tuples = [('load', country, 'load', 'ENTSO-E', url) for country in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    
    return df


def read_hertz(filepath, variable_name, url, headers):
    """
    Read a .csv file with wind or solar power timeseries data from 
    50Hertz into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    tech_attribute: str
        Technology_attribute of the data, e.g. ``wind_forecast`` 
    url : str
        URL linking to the source website where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
        
    # Since 2016, wind data has an aditional column for offshore
    
    # JONATHAN: Better change the variable-names for 50Hertz to 
    # thios order: 'wind-with-onshore_generation' (?)
    # JONATHAN: Baltic 1 has been producing since 2011-05-02 and Baltic2 since
    # early 2015 (source: Wikipedia) so it is probably not correct that 50Hertz-Wind
    # data pre-2016 is only onshore. Maybe we can ask at 50Hertz directly.
    if variable_name.split('_')[0] == 'wind': 
        #import pdb; pdb.set_trace()
        tech, attribute, phase = variable_name.split('_')
        cols = ['date', 'time', attribute + '_wind-onshore']
        tuples = [('wind-onshore', 'DE-50Hertz', attribute, '50Hertz', url)]
        usecols=[0, 1, 3]
        
        if phase == 'with-offshore':
            cols.append(attribute + '_wind-offshore')
            tuples.append(('wind-offshore', 'DE-50Hertz', attribute, '50Hertz', url))
            usecols=[0, 1, 4, 5]
    
    elif variable_name.split('_')[0] == 'solar':
        tech, attribute = variable_name.split('_')
        cols = ['date', 'time', attribute]
        tuples = [('solar', 'DE-50Hertz', attribute, '50Hertz', url)]
        usecols=[0, 1, 3]
        
    df = pd.read_csv(
        filepath,
        sep=';',
        header=3,
        index_col='timestamp',
        names=cols,
        parse_dates={'timestamp': ['date', 'time']},
        date_parser=None,
        dayfirst=True,
        decimal=',',
        thousands='.',
        # truncate values in 'time' column after 5th character
        converters={'time': lambda x: x[:5]},
        usecols=usecols,
    )
    
    # Until 2006 as well as  in 2015, during the fall dst-transistion, only the 
    # wintertime hour (marked by a B in the data) is reported, the summertime 
    # hour, (marked by an A) is missing in the data.  
    # dst_arr is a boolean array consisting only of "False" entries, telling 
    # python to treat the hour from 2:00 to 2:59 as wintertime.
    if pd.to_datetime(df.index.values[0]).year not in range(2007,2015):
        dst_arr = np.zeros(len(df.index), dtype=bool)
        df.index = df.index.tz_localize('Europe/Berlin', ambiguous=dst_arr)
    else:
        df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    df.index = df.index.tz_convert(None)
    
    # Create the MultiIndex
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    
    return df


def read_amprion(filepath, variable_name, url, headers):
    """
    Read a .csv file with wind or solar power timeseries data from 
    Amprion into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    variable_name : str
        Name of variable, e.g. ``solar``
    url : str
        URL linking to the source website where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
    df = pd.read_csv(
        filepath,
        sep=';',
        header=0,
        index_col='timestamp',
        names=['date', 'time', 'forecast', 'generation'],
        parse_dates={'timestamp' : ['date', 'time']},
        date_parser=None,
        dayfirst=True,
        decimal=',',
        thousands=None,
        # Truncate values in 'time' column after 5th character.
        converters={'time': lambda x: x[:5]},
        usecols=[0, 1, 2, 3],        
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

    # Create the MultiIndex
    tuples = [(variable_name, 'DE-Amprion', attribute, 'Amprion', url)
              for attribute in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)

    return df


def read_tennet(filepath, variable_name, url, headers):
    """
    Read a .csv file with wind or solar power timeseries data from 
    TenneT DE into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    variable_name : str
        Name of variable, e.g. ``solar``
    url : str
        URL linking to the source website where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
    if variable_name == 'solar':
        cols = [0, 1, 2, 3]
        colnames = ['date', 'pos', 'forecast', 'generation']
    if variable_name == 'wind':
        cols = [0, 1, 2, 3, 4]
        colnames = ['date', 'pos', 'forecast', 'wind-total', 'wind-offshore']
        
    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='latin_1',
        header=3,
        index_col=None,
        names=colnames,
        parse_dates=False,
        date_parser=None,
        dayfirst=True,
        thousands=None,
        converters=None,          
        usecols=cols,
    )

    df['date'].fillna(method='ffill', limit = 100, inplace=True)

    # Check the rows for irregularities
    for i in range(len(df.index)):
        # On the day in March when summertime begins, shift the data forward by
        # 1 hour, beginning with the 9th quarter-hour, so the index runs again
        # up to 96
        if (df['pos'][i] == 92 and ((i == len(df.index)-1) or (df['pos'][i + 1] == 1))):
            slicer = df[(df['date'] == df['date'][i]) & (df['pos'] >= 9)].index
            df.loc[slicer, 'pos'] = df['pos'] + 4

        if df['pos'][i] > 96: # True when summertime ends in October
            logger.info('%s th quarter-hour at %s, position %s',
                        df['pos'][i], df.ix[i,'date'], (i))  

            # Instead of having the quarter-hours' index run up to 100, we want 
            # to have it set back by 1 hour beginning from the 13th
            # quarter-hour, ending at 96
            if df['pos'][i] == 100 and not (df['pos'] == 101).any():                  
                slicer = df[(df['date'] == df['date'][i]) & (df['pos'] >= 13)].index
                df.loc[slicer, 'pos'] = df['pos'] - 4                     

            # In 2011 and 2012, there are 101 quarter hours on the day the 
            # summertime ends, so 1 too many.  From looking at the data, we
            # inferred that the 13'th quarter hour is the culprit, so we drop
            # that.  The following entries for that day need to be shifted.
            elif df['pos'][i] == 101: 
                df = df[~((df['date'] == df['date'][i]) & (df['pos'] == 13))]
                slicer = df[(df['date'] == df['date'][i]) & (df['pos'] >= 13)].index
                df.loc[slicer, 'pos'] = df['pos'] - 5     

    # On 2012-03-25, there are 94 entries, where entries 8 and 10 are probably
    # wrong.
    if df['date'][0] == '2012-03-01':
        df = df[~((df['date'] == '2012-03-25') & 
                  ((df['pos'] == 8) | (df['pos'] == 10)))]
        slicer = df[(df['date'] == '2012-03-25') & (df['pos'] >= 9)].index
        df.loc[slicer, 'pos'] = [8] + list(range(13, 97))        

    # On 2012-09-27, there are 97 entries.  Probably, just the 97th entry is wrong.
    if df['date'][0] == '2012-09-01':
        df = df[~((df['date'] == '2012-09-27') & (df['pos'] == 97))]          

    # Here we compute the timestamp from the position and generate the
    # datetime-index
    df['hour'] = (np.trunc((df['pos']-1)/4)).astype(int).astype(str)
    df['minute'] = (((df['pos']-1)%4)*15).astype(int).astype(str)
    df['timestamp'] = pd.to_datetime(df['date'] + ' ' + df['hour'] + ':' +
                                     df['minute'], dayfirst = True)
    df.set_index('timestamp',inplace=True)

    # In the years 2006, 2008, and 2009, the dst-transition hour in March
    # appears as empty rows in the data.  We delete it from the set in order to
    # make the timezone localization work.  
    for crucial_date in pd.to_datetime(
        ['2006-03-26', '2008-03-30', '2009-03-29']).date:
        if df.index[0].year == crucial_date.year:
            df = df[~((df.index.date == crucial_date) &
                      (df.index.hour == 2))]

    df.drop(['pos', 'date', 'hour', 'minute'], axis=1, inplace=True)

    df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    df.index = df.index.tz_convert(None)

    # Create the MultiIndex
    if variable_name == 'solar':
        tuples = [('solar', 'DE-TenneT', 'forecast', 'TenneT', url),
                  ('solar', 'DE-TenneT', 'generation', 'TenneT', url)]
    if variable_name == 'wind': # offshore generation starts 2009-09-20
        tuples = [('wind-total', 'DE-TenneT', 'forecast', 'TenneT', url),
                  ('wind-total', 'DE-TenneT', 'generation', 'TenneT', url),                
                  ('wind-offshore', 'DE-TenneT', 'generation', 'TenneT', url)]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    
    return df


def read_transnetbw(filepath, variable_name, url, headers):
    """
    Read a .csv file with wind or solar power timeseries data from 
    TransnetBW into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    variable_name : str
        Name of variable, e.g. ``solar`
    url : str
        URL linking to the source website where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
    df = pd.read_csv(
        filepath,
        sep=';',
        header=0,
        index_col='timestamp',
        names=['date', 'time', 'forecast', 'generation'],
        parse_dates={'timestamp': ['date', 'time']},
        date_parser=None,         
        dayfirst=True,
        decimal=',',
        thousands=None,
        converters=None,
        usecols=[2, 3, 4, 5],  # 0-indexed, i.e. "2" refers to the 3rd column
    )
    
    # 'ambigous' refers to how the October dst-transition hour is handled.  
    # ‘infer’ will attempt to infer dst-transition hours based on order.
    df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    df.index = df.index.tz_convert(None)
    
    # The 2nd column represents the start and the 4th the end of the respective 
    # period. The former has some errors, so we use the latter to construct the 
    # index and shift the data back by 1 period.  
    df = df.shift(periods=-1, freq='15min', axis='index')
    
    # Create the MultiIndex
    tuples = [(variable_name, 'DE-TransnetBW', attribute, 'TransnetBW', url)
              for attribute in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    
    return df


def read_capacities(filepath, url, headers):
    """
    Read a .csv file with capacity timeseries data from the OPSD renewables
    datapacke into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    url : str
        URL linking to the source website where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
    df = pd.read_csv(
        filepath,
        sep=',',
        header=0,
        index_col='timestamp',
        names=['timestamp', 'wind-total', 'solar'],
        parse_dates=True,
        date_parser=None,         
        dayfirst=True,
        decimal='.',
        thousands=None,
        converters=None,
        usecols=[0,2,3],
    )
    
    # The capacities data only has one entry per day, which pandas 
    # interprets as 00:00h. We will broadcast the dayly data for 
    # all quarter-hours of the day until the next given data point.
    # For this, we we expand the index so it reaches to 23:59 of 
    # the last day, not only 00:00.
    last = pd.to_datetime([df.index[-1].replace(hour=23, minute=59)]) 
    until_last = df.index.append(last).rename('timestamp')
    df = df.reindex(index=until_last, method='ffill')
    df.index = df.index.tz_localize('Europe/Berlin')
    df.index = df.index.tz_convert(None)
    df = df.resample('15min').ffill()

    # Create the MultiIndex
    tuples = [(tech, 'DE', 'capacity', 'OPSD datapackage_renewables', url)
              for tech in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    
    return df


def read_svenska_kraftnaet(filePath, variable_name, url, headers):
    """
    Read a .xls file with wind and solar power timeseries data from 
    Svenska Kraftnaet into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    variable_name : str
        Name of variable, e.g. ``solar`
    url : str
        URL linking to the source website where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
    if variable_name in ['wind_solar_1', 'wind_solar_2']:
        skip = 4
        cols = [0,1,3]
        colnames = ['date', 'hour', 'wind-total']
    else:
        if variable_name == 'wind_solar_4':
            skip = 5
        else:
            skip = 7
        cols = [0,2,8]
        colnames = ['timestamp', 'wind-total', 'solar']
        
    df = pd.read_excel(
        io = filePath,
        # read the last sheet (in some years,
        # there are hidden sheets that would cause errors)
        sheetname=-1,
        header=None,
        skiprows=skip,
        index_col=None,
        parse_cols=cols
    )

    df.columns = colnames
    
    if variable_name in ['wind_solar_1', 'wind_solar_2']:
        #in 2009 there is a row below the table for the sums that we don't want to read in
        df = df[df['date'].notnull()] 
        df['timestamp'] = pd.to_datetime(
            df['date'].astype(int).astype(str) + ' ' +
            df['hour'].astype(int).astype(str).str.replace('00','') + ':00',
            dayfirst = False,
            infer_datetime_format = True)
        df.drop(['date','hour'], axis=1, inplace = True)
    else:
        #in 2011 there is a row below the table for the sums that we don't want to read in
        df = df[((df['timestamp'].notnull()) &
                 (df['timestamp'].astype(str) != 'Tot summa GWh'))] 
        df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst = True) 
        
    df.set_index('timestamp', inplace=True)
    # The timestamp ("Tid" in the original) gives the time without 
    # daylight savings time adjustments (normaltid). To convert to UTC,
    # one hour has to be deducted
    df.index = df.index + pd.offsets.Hour(-1)   
    
    # Create the MultiIndex
    tuples = [(tech, 'SE', 'generation', 'Svenska Kraftnaet', url)
              for tech in df.columns]
    df.columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    
    return df


def read(sources_yaml_path, out_path, headers, subset=None):
    """
    For the sources specified in the sources.yml file, pass each downloaded
    file to the correct read function.
    Parameters
    ----------
    sources_yaml_path : str
        Filepath of sources.yml
    out_path : str
        Base download directory in which to save all downloaded files.    
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.
    subset : list or iterable, optional
        If given, specifies a subset of data sources to download,
        e.g.: ['TenneT', '50Hertz'].
        
    """
#    data_sets = {'15min': pd.DataFrame(), '60min': pd.DataFrame()}

    with open(sources_yaml_path, 'r') as f:
        sources = yaml.load(f.read())

    # If subset is given, only keep source_name keys in subset
    if subset is not None:
        sources = {k: v for k, v in sources.items() if k in subset}
    
    # For each source in the source dictionary
    for source_name, source_dict in sources.items():
        # For each variable from source_name
        for variable_name, param_dict in source_dict.items():
            variable_dir = os.path.join(out_path, source_name, variable_name)
            # Check if there are folders for variable_name
            if not os.path.exists(variable_dir):
                logger.info('folder not found for %s, %s', source_name, variable_name)
            else:
                # For each file downloaded for that variable
                for container in os.listdir(variable_dir):
                    files = os.listdir(os.path.join(variable_dir, container))
                    # Check if there is only one file per folder
                    if not len(files) == 1:
                        logger.info('error: found more than one file in %s %s %s', # JONATHAN Or less?
                                    source_name, variable_name, container)
                    else:                        
                        logger.info('reading data:\n\t '
                                    'Source:   %s\n\t '
                                    'Variable: %s\n\t '
                                    'Filename: %s',
                                    source_name, variable_name, files[0])
                        
                        filepath = os.path.join(variable_dir, container, files[0])
                        
                        # Check if file is not empty
                        if os.path.getsize(filepath) < 128:
                            logger.info(
                                'file is smaller than 128 Byte, which means it is probably empty'
                            )
                        else:
                            url = param_dict['web']
                            if source_name == 'OPSD':
                                data_to_add = read_capacities(filepath, url, headers)
                            elif source_name == 'CEPS':
                                data_to_add = read_ceps(filepath, variable_name, url, headers)
                            elif source_name == 'ENTSO-E':
                                data_to_add = read_entso(filepath, url, headers)
                            elif source_name == 'Energinet.dk':
                                data_to_add = read_energinet_dk(filepath, url, headers)
                            elif source_name == 'Elia':
                                data_to_add = read_elia(filepath, variable_name, url, headers)
                            elif source_name == 'PSE':
                                data_to_add = read_pse(filepath, variable_name, url, headers)                            
                            elif source_name == 'RTE':
                                data_to_add = read_rte(filepath, variable_name, url, headers)
                            elif source_name == 'Svenska Kraftnaet':
                                data_to_add = read_svenska_kraftnaet(filepath, variable_name, url, headers)
                            elif source_name == '50Hertz':
                                data_to_add = read_hertz(filepath, variable_name, url, headers)
                            elif source_name == 'Amprion':
                                data_to_add = read_amprion(filepath, variable_name, url, headers)
                            elif source_name == 'TenneT':
                                data_to_add = read_tennet(filepath, variable_name, url, headers)
                            elif source_name == 'TransnetBW':
                                data_to_add = read_transnetbw(filepath, variable_name, url, headers)

                            # cut off data_to_add at end of year (I fogot why I would want that):
                            # data_to_add = data_to_add[:'2015-12-31 22:45:00']

                            yield param_dict['resolution'], data_to_add
                            
#                            key = param_dict['resolution']
#                            if len(data_sets[key]) == 0:
#                                data_sets[key] = data_to_add
#                            else:
#                                data_sets[key] = data_sets[key].combine_first(data_to_add)

            
#            #reindex with a synthetic index that is sure to be continous in order to expose gaps in the data
#            no_gaps = pd.DatetimeIndex(start=data_sets[param_dict['resolution']].index[0],
#                                       end=data_sets[param_dict['resolution']].index[-1],
#                                       freq=param_dict['resolution'])
#            data_sets[param_dict['resolution']] = data_sets[param_dict['resolution']].reindex(index=no_gaps)
#            
#    return data_sets