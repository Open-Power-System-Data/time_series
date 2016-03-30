
# coding: utf-8

# # 1. Read

# Part of the project [Open Power System Data](http://open-power-system-data.org/).
# 
# Find the latest version of this notebook an [GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/download.read)
# 
# Go back to the main notebook ([GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/main.ipynb?) / [local copy](main.ipynb))
# 
# This notebook reads the data saved by the download script ([GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/download.ipynb) / [local copy](download.ipynb))

# # Table of Contents
# * [1. Read](#1.-Read)
# * [2. Preparations](#2.-Preparations)
# 	* [2.1 Libraries](#2.1-Libraries)
# 	* [2.2 Set up a log.](#2.2-Set-up-a-log.)
# 	* [2.3 Locate the download directory](#2.3-Locate-the-download-directory)
# 	* [2.4 Set the level names of the MultiIndex](#2.4-Set-the-level-names-of-the-MultiIndex)
# * [3. read-functions for individual data sources](#3.-read-functions-for-individual-data-sources)
# 	* [3.1 ENTSO-E](#3.1-ENTSO-E)
# 	* [3.2 '50Hertz](#3.2-'50Hertz)
# 	* [3.3 Amprion](#3.3-Amprion)
# 	* [3.4 TenneT](#3.4-TenneT)
# 	* [3.5 TransnetBW](#3.5-TransnetBW)
# 	* [3.6 Capacities](#3.6-Capacities)
# * [4. Read files one by one](#4.-Read-files-one-by-one)
# 	* [4.1 Create empty DataFrames](#4.1-Create-empty-DataFrames)
# 	* [4.2 Apply the processing function one-by-one](#4.2-Apply-the-processing-function-one-by-one)
# * [5. Write the data to disk for further processing](#5.-Write-the-data-to-disk-for-further-processing)
# 

# # 2. Preparations

# ## 2.1 Libraries

# Loading some python libraries.

# In[ ]:

from datetime import datetime, date, timedelta
import pytz
import yaml
import os
import numpy as np
import pandas as pd
import logging


# ## 2.2 Set up a log.

# In[ ]:

logger = logging.getLogger('log')
logger.setLevel('INFO')


# ## 2.3 Locate the download directory

# Set the local path where the input data is stored. This script expects a file structure acording to the following schema:
# * \working_directory\downloadpath\source\resource\container\file.csv
# 
# for example:
# * \datapackage_timeseries\original_data\TransnetBW\wind\2010-01-01_2010-01-31\mwindeinsp_ist_prognose_2010_01.csv

# In[ ]:

downloadpath = 'original_data'


# ## 2.4 Set the level names of the MultiIndex

# These are the rows at the top of the data used to store metadata internally. In the output data created by the processing script ([local copy](processing.ipynb#) / [GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/processing.ipynb)), this information will be moved to the [datapackage.json](datapackage.json#) File.

# In[ ]:

HEADERS = ['variable', 'country', 'attribute', 'source', 'web']


# # 3. read-functions for individual data sources

# ## 3.1 ENTSO-E

# In[ ]:

def read_entso(filepath, web):
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
    columns = pd.MultiIndex.from_tuples(tuples, names=HEADERS)
    df.columns = columns
    
    return df


# ## 3.2 '50Hertz

# In[ ]:

def read_hertz(filepath, tech_attribute, web):
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


# ## 3.3 Amprion

# In[ ]:

def read_amprion(filepath, tech, web):
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


# ## 3.4 TenneT

# The Tennet Data doesn't feature a time column. Instead, the quarter-hourly data entries for each day are numbered by their position, creating an index ranging...
# * from 1 to 96 on normal days,
# * from 1 to 92 on spring dst-transition dates,
# * from 1 to 100 on fall dst-transition days.
# 
# This index can be used to compute a timestamp. However, there are a couple of errors in the data, which is why a lot of exceptions need to be specified.

# In[ ]:

def read_tennet(filepath, tech, web):
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


# ## 3.5 TransnetBW

# In[ ]:

def read_transnetbw(filepath, tech, web):
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


# ## 3.6 Capacities

# In[ ]:

def read_capacities(filepath, web):
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
    source = 'own calculation'
    tuples = [(tech, 'DE', 'capacity', source, web) for tech in df.columns]
    columns = pd.MultiIndex.from_tuples(tuples, names=HEADERS)
    df.columns = columns
    
    return df


# # 4. Read files one by one

# ## 4.1 Create empty DataFrames

# We create a dictionary with an empty DataFrame each for data with 15/60 minute resolution. This line deletes all data previously loaded into the data_sets.

# In[ ]:

data_sets = {'15min': pd.DataFrame(), '60min': pd.DataFrame()}


# ## 4.2 Apply the processing function one-by-one

# For each source/TSO and technology specified in the conf dict, this section finds all the downloaded files in the downloads folder and then calls the matching read function.
# The datasets returned by the read function are then merged with the other data of the same resolution.

# This section contains a python dictionary indicating which datasources there are, which data types they provide and a link to the source to be included in the columnd header.

# In[ ]:

conf = """
60min:
    ENTSO-E:
        load: https://www.entsoe.eu/data/data-portal/consumption/Pages/default.aspx #Hourly load values of all countries for a specific month
15min:
    50Hertz: 
        wind_generation: http://www.50hertz.com/en/Grid-Data/Wind-power/Archive-Wind-power
        wind_forecast: http://www.50hertz.com/en/Grid-Data/Wind-power/Archive-Wind-power
        solar_generation: http://www.50hertz.com/en/Grid-Data/Photovoltaics/Archive-Photovoltaics
        solar_forecast: http://www.50hertz.com/en/Grid-Data/Photovoltaics/Archive-Photovoltaics
    Amprion:
        wind: http://www.amprion.net/en/wind-feed-in
        solar: http://www.amprion.net/en/photovoltaic-infeed
    TenneT:
        wind: http://www.tennettso.de/site/en/Transparency/publications/network-figures/actual-and-forecast-wind-energy-feed-in
        solar: http://www.tennettso.de/site/en/Transparency/publications/network-figures/actual-and-forecast-photovoltaic-energy-feed-in
    TransnetBW:
        wind: https://www.transnetbw.com/en/key-figures/renewable-energies/wind-infeed
        solar: https://www.transnetbw.com/en/key-figures/renewable-energies/photovoltaic
    OPSD:
        capacities: http://data.open-power-system-data.org/datapackage_renewables/
"""
conf = yaml.load(conf)


# In[ ]:

for resolution, sources in conf.items():
    for source, resources in sources.items():
        for resource, web in resources.items():
            resource_dir = os.path.join(downloadpath, source, resource)
            if not os.path.exists(resource_dir):
                logger.info('folder not found for %s, %s', source, resource)
            else:
                for container in os.listdir(resource_dir):
                    files = os.listdir(os.path.join(resource_dir, container))
                    if not len(files) == 1:
                        logger.info('error: found more than one file in %s %s %s',
                                    source, resource, container)
                    else:                        
                        logger.info('reading %s %s %s',
                                    source, resource, files[0])
                        filepath = os.path.join(resource_dir, container, files[0])
                        if os.path.getsize(filepath) < 128:
                            logger.info('file is smaller than 128 Byte,' +
                                    'which means it is probably empty')
                        else:
                            if source == 'ENTSO-E':
                                data_to_add = read_entso(filepath, web)
                            elif source == 'Svenska_Kraftnaet':
                                data_to_add = read_svenskakraftnaet(filepath, source, resource, web)
                            elif source == '50Hertz':
                                data_to_add = read_hertz(filepath, resource, web)
                            elif source == 'Amprion':
                                data_to_add = read_amprion(filepath, resource, web)
                            elif source == 'TenneT':
                                data_to_add = read_tennet(filepath, resource, web)
                            elif source == 'TransnetBW':
                                data_to_add = read_transnetbw(filepath, resource, web)
                            elif source == 'OPSD':
                                data_to_add = read_capacities(filepath, web)
                            
                            # cut off data_to_add at end of year:
                                data_to_add = data_to_add[:'2015-12-31 22:45:00']

                            if len(data_sets[resolution]) == 0:
                                data_sets[resolution] = data_to_add
                            else:
                                data_sets[resolution] =                                 data_sets[resolution].combine_first(data_to_add)


# # 5. Write the data to disk for further processing

# In[ ]:

for resolution, data_set in data_sets.items():
    data_set.to_csv('raw_data_' + resolution + '.csv', float_format='%.2f')


# The data should now be processed further. using the processing script ([GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/processing.ipynb) / [local copy](processing.ipynb))
