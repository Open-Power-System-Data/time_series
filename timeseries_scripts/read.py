"""
Open Power System Data

Timeseries Datapackage

read.py : read time series files

"""

from datetime import datetime, date, timedelta
import pytz
import yaml
import os
import numpy as np
import pandas as pd
import logging


def read_entso(filepath, web, headers):
    """
    Read a .xls file with hourly load data from the ENTSO Data Portal
    into a dataframe. Returns a pandas.DataFrame.

    Parameters
    ----------
    filepath : str
        Directory path of file to be read.
    web : str
        URL linking to the source website where this data comes from.
    headers : list
        List of strings indicating the level names of the pandas.MultiIndex
        for the columns of the dataframe.

    """
    df = pd.read_excel(
        io=filepath,
        header=9,
        skiprows=None,
        index_col=[0, 1], # create MultiIndex from first 2 columns ['Country', 'Day']
        parse_cols = None # None means: parse all columns
        )
    
    # Create a list of daylight savings transitions 
    dst_transitions = [d.replace(hour=2) for d in pytz.timezone(
        'Europe/Berlin')._utc_transition_times[1:]]
    
    #import pdb; pdb.set_trace()
    
    df.columns.names = ['raw_hour']
    
    # The original data has days and countries in the rows and hours in the
    # columns.  This rearranges the table, mapping hours on the rows and
    # countries on the columns.  
    df = df.stack(level='raw_hour').unstack(level='Country').reset_index()    
    
    # Truncate the hours column after 2 characters and replace letters 
    # which are there to indicate the order during fall dst-transition.      
    df['hour'] = df['raw_hour'].str[:2].str.replace('A','').str.replace('B','')
    # Hours are indexed 1-24 by ENTSO-E, but pandas requires 0-23, so we deduct 1.
    df['hour'] = (df['hour'].astype(int) - 1).astype(str)
    
    df['timestamp'] = pd.to_datetime(df['Day']+' '+df['hour']+':00')
    df.set_index('timestamp', inplace=True)    
    
    # Drop 2nd occurence of 03:00 appearing in October data except for autumn
    # dst-transition.  
    df = df[~((df['raw_hour'] == '3B:00:00') & ~ (df.index.isin(dst_transitions)))]
    
    # Drop 03:00 for (spring) dst-transition. October data is unaffected because
    # the format is 3A:00/3B:00.  
    df = df[~((df['raw_hour'] == '03:00:00') & (df.index.isin(dst_transitions)))]
    
    df.drop(['Day', 'hour', 'raw_hour'], axis=1, inplace=True)
    df.index = df.index.tz_localize('Europe/Brussels', ambiguous='infer')
    df.index = df.index.tz_convert(None)
    
    df.rename(columns={'DK_W': 'DKw', 'UA_W': 'UAw'}, inplace=True)
    
    # replace strings indicating missing data with proper NaN-format.  
    df = df.replace(to_replace='n.a.', value=np.nan)
    
    # Create the MultiIndex.  
    tuples = [('load', country, 'load', 'ENTSO-E', web) for country in df.columns]
    columns = pd.MultiIndex.from_tuples(tuples, names=headers)
    df.columns = columns
    
    return df


def read_hertz(filepath, tech_attribute, web, HEADERS):
    tech = tech_attribute.split('_')[0]
    attribute = tech_attribute.split('_')[1]
    df = pd.read_csv(
        filepath,
        sep=';',
        header=3,
        index_col='timestamp',
        names=['date',
               'time',
               attribute],
        parse_dates={'timestamp': ['date', 'time']},
        date_parser=None,
        dayfirst=True,
        decimal=',',
        thousands='.',
        # truncate values in 'time' column after 5th character
        converters={'time': lambda x: x[:5]},
        usecols=[0, 1, 3],
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
    tuples = [(tech, 'DE50hertz', attribute, '50Hertz', web)]
    columns = pd.MultiIndex.from_tuples(tuples, names=HEADERS)
    df.columns = columns
    
    return df


def read_amprion(filepath, tech, web, HEADERS):
    df = pd.read_csv(
        filepath,
        sep=';',
        header=0,
        index_col='timestamp',
        names=['date',
               'time',
               'forecast',
               'generation'],
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
    tuples = [(tech, 'DEamprion', attribute, 'Amprion', web) for attribute in df.columns]
    columns = pd.MultiIndex.from_tuples(tuples, names=HEADERS)
    df.columns = columns    

    return df


def read_tennet(filepath, tech, web, HEADERS):
    df = pd.read_csv(
        filepath,
        sep=';',
        encoding='latin_1',
        header=3,
        index_col=None,
        names=['date',
               'pos',
               'forecast',
               'generation'],
        parse_dates=False,
        date_parser=None,
        dayfirst=True,
        thousands=None,
        converters=None,          
        usecols=[0, 1, 2, 3],
    )

    df['date'].fillna(method='ffill', limit = 100, inplace=True)

    for i in range(len(df.index)):
        # On the day in March when summertime begins, shift the data forward by
        # 1 hour, beginning with the 9th quarter-hour, so the index runs again
        # up to 96
        if (df['pos'][i] == 92 and
            ((i == len(df.index)-1) or (df['pos'][i + 1] == 1))):
            slicer = df[(df['date'] == df['date'][i]) & (df['pos'] >= 9)].index
            df.loc[slicer, 'pos'] = df['pos'] + 4

        if df['pos'][i] > 96: # True when summertime ends in October
            logger.info('%s th quarter-hour at %s, position %s',
                        df['pos'][i], df.ix[i,'date'], (i))  

            # Instead of having the quarter-hours' index run up to 100, we want 
            # to have it set back by 1 hour beginning from the 13th
            # quarter-hour, ending at 96
            if (df['pos'][i] == 100 and not (df['pos'] == 101).any()):                    
                slicer = df[(df['date'] == df['date'][i]) & (df['pos'] >= 13)].index
                df.loc[slicer, 'pos'] = df['pos'] - 4                     

            # In 2011 and 2012, there are 101 qaurter hours on the day the 
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
    for crucial_date in pd.to_datetime(['2006-03-26', '2008-03-30',
                                        '2009-03-29']).date:
        if df.index[0].year == crucial_date.year:
            df = df[~((df.index.date == crucial_date) &
                          (df.index.hour == 2))]

    df.drop(['pos', 'date', 'hour', 'minute'], axis=1, inplace=True)

    df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    df.index = df.index.tz_convert(None)
    
    # Create the MultiIndex
    tuples = [(tech, 'DEtennet', attribute, 'TenneT', web) for attribute in df.columns]
    columns = pd.MultiIndex.from_tuples(tuples, names=HEADERS)
    df.columns = columns
    
    return df


def read_transnetbw(filepath, tech, web, HEADERS):
    df = pd.read_csv(
        filepath,
        sep=';',
        header=0,
        index_col='timestamp',
        names=['date',
               'time',
               'forecast',
               'generation'],
        parse_dates={'timestamp': ['date', 'time']},
        date_parser=None,         
        dayfirst=True,
        decimal=',',
        thousands=None,
        converters=None,
        usecols=[2, 3, 4, 5],
    )
    
    # 'ambigous' refers to how the October dst-transition hour is handled.  
    # ‘infer’ will attempt to infer dst-transition hours based on order.
    df.index = df.index.tz_localize('Europe/Berlin', ambiguous='infer')
    df.index = df.index.tz_convert(None)
    
    # The time taken from column 3 indicates the end of the respective period.
    # to construct the index, however, we need the beginning, so we shift the 
    # data back by 1 period.  
    df = df.shift(periods=-1, freq='15min', axis='index')
    
    # Create the MultiIndex
    tuples = [(tech, 'DEtransnetbw', attribute, 'TransnetBW', web) for attribute in df.columns]
    columns = pd.MultiIndex.from_tuples(tuples, names=HEADERS)
    df.columns = columns
    
    return df


def read_capacities(filepath, web, HEADERS):
    df = pd.read_csv(
        filepath,
        sep=',',
        header=0,
        index_col='timestamp',
        names=['timestamp',
               'wind',
               'solar'],
        parse_dates=True,
        date_parser=None,         
        dayfirst=True,
        decimal='.',
        thousands=None,
        converters=None,
        usecols=[0,2,3],
    )
    
    last = pd.to_datetime([df.index[-1].replace(hour=23, minute=59)])
    until_last = df.index.append(last).rename('timestamp')
    df = df.reindex(index=until_last, method='ffill')
    df.index = df.index.tz_localize('Europe/Berlin')
    df.index = df.index.tz_convert(None)
    df = df.resample('15min').ffill()
    
    # Create the MultiIndex
    tuples = [(tech, 'DE', 'capacity', 'own calculation', web) for tech in df.columns]
    columns = pd.MultiIndex.from_tuples(tuples, names=HEADERS)
    df.columns = columns
    
    return df


def read(sources_yaml_path, out_path, headers, subset=None):

    data_sets = {'15min': pd.DataFrame(), '60min': pd.DataFrame()}

    with open(sources_yaml_path, 'r') as f:
        sources = yaml.load(f.read())

    # If subset is given, only keep source_name keys in subset
    if subset is not None:
        sources = {k: v for k, v in sources.items() if k in subset}

    for source_name, source_dict in sources.items():
        for variable_name, param_dict in source_dict.items():
            variable_dir = os.path.join(out_path, source_name, variable_name)
            if not os.path.exists(variable_dir):
                logging.info('folder not found for %s, %s', source_name, variable_name)
            else:
                for container in os.listdir(variable_dir):
                    files = os.listdir(os.path.join(variable_dir, container))
                    if not len(files) == 1:
                        logging.info('error: found more than one file in %s %s %s',
                                    source_name, variable_name, container)
                    else:                        
                        logging.info('reading %s %s %s',
                                    source_name, variable_name, files[0])
                        filepath = os.path.join(variable_dir, container, files[0])
                        if os.path.getsize(filepath) < 128:
                            logging.info('file is smaller than 128 Byte,' +
                                        'which means it is probably empty')
                        else:
                            if source_name == 'ENTSO-E':
                                data_to_add = read_entso(filepath, param_dict['web'], headers)
                            elif source_name == 'Svenska_Kraftnaet':
                                data_to_add = read_svenskakraftnaet(filepath, source_name, variable_name, param_dict['web'], headers)
                            elif source_name == '50Hertz':
                                data_to_add = read_hertz(filepath, variable_name, param_dict['web'], headers)
                            elif source_name == 'Amprion':
                                data_to_add = read_amprion(filepath, variable_name, param_dict['web'], headers)
                            elif source_name == 'TenneT':
                                data_to_add = read_tennet(filepath, variable_name, param_dict['web'], headers)
                            elif source_name == 'TransnetBW':
                                data_to_add = read_transnetbw(filepath, variable_name, param_dict['web'], headers)
                            elif source_name == 'OPSD':
                                data_to_add = read_capacities(filepath, param_dict['web'], headers)

                            # cut off data_to_add at end of year:
                                data_to_add = data_to_add[:'2015-12-31 22:45:00']

                            if len(data_sets[param_dict['resolution']]) == 0:
                                data_sets[param_dict['resolution']] = data_to_add
                            else:
                                data_sets[param_dict['resolution']] = \
                                data_sets[param_dict['resolution']].combine_first(data_to_add)
    return data_sets