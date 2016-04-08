
# coding: utf-8

# # 1. Processing

# Part of the project [Open Power System Data](http://open-power-system-data.org/).
# 
# Find the latest version of this notebook an [GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/processing.ipynb)
# 
# Go back to the main notebook ([GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/main.ipynb) / [local copy](main.ipynb))
# 
# This notebook processes the data combined by the read script ([GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/read.ipynb) / [local copy](read.ipynb)).

# # Table of Contents
# * [1. Processing](#1.-Processing)
# * [2. Preparations](#2.-Preparations)
# 	* [2.1 Libraries](#2.1-Libraries)
# 	* [2.2 Set up a log.](#2.2-Set-up-a-log.)
# 	* [2.3 Load raw data](#2.3-Load-raw-data)
# * [3. Own calculations](#3.-Own-calculations)
# 	* [3.1 Missing Data Handling](#3.1-Missing-Data-Handling)
# 	* [3.2 Aggregate German data from individual TSOs](#3.2-Aggregate-German-data-from-individual-TSOs)
# 	* [3.3 Create hourly data from 15' data](#3.3-Create-hourly-data-from-15'-data)
# * [4. Create metadata](#4.-Create-metadata)
# 	* [4.1 General metadata](#4.1-General-metadata)
# 	* [4.2 Columns-specific metadata](#4.2-Columns-specific-metadata)
# * [5. Write the data to disk](#5.-Write-the-data-to-disk)
# 	* [5.1 Write to SQL-database](#5.1-Write-to-SQL-database)
# 	* [5.2 Write to Excel](#5.2-Write-to-Excel)
# 	* [5.3 Write to CSV](#5.3-Write-to-CSV)
# * [6. Plausibility checks](#6.-Plausibility-checks)
# 

# # 2. Preparations

# ## 2.1 Libraries

# This notebook makes use of the [pycountry](https://pypi.python.org/pypi/pycountry) library that is not part of Anaconda. Install it with with `pip install pycountry` from your command line.

# In[ ]:

from datetime import timedelta
import pandas as pd
import numpy as np
import logging
import pycountry
import json
import sqlite3
import copy
from itertools import chain
import logging


# In[ ]:

HEADERS = ['variable', 'country', 'attribute', 'source', 'web']


# ## 2.2 Set up a log.

# In[ ]:

logger = logging.getLogger('log')
logger.setLevel('INFO')


# ## 2.3 Load raw data

# Load the dataset compiled by the read-script ([local copy](read.ipynb#) / [GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/read.ipynb))

# In[ ]:

data_sets = {}
for res_key in ['15min', '60min']:
    filename = 'raw_data_' + res_key + '.csv'
    try:
        data_sets[res_key] = pd.read_csv(
            filename,
            header=[0,1,2,3,4],
            index_col=0,
            parse_dates=True
            )
    except Exception:
        logging.error('Error reading file: {}'.format(filename))


# # 3. Own calculations

# ## 3.1 Missing Data Handling

# Patch missing data. At this stage, only implemented for 15 minute resolution solar/wind in-feed data from german TSOs. Small gaps (up to 2 hours) are filled by linear interpolation. For the generation timeseries, larger gaps are guessed by up-/down scaling the data from other balancing areas to fit the expected magnitude of the missing data.
# 
# The locations of missing data are stored in the nan_table DataFrame.

# In[ ]:

def patcher(frame):
    '''Search for missing values in a DataFrame and apply custom patching.'''
    nan_table = pd.DataFrame()
    patched = pd.DataFrame()
    one_period = frame.index[1] - frame.index[0]
    for col_name, col in frame.iteritems():
        df = col.to_frame()

        # tag all occurences of NaN in the data (but not before first actual entry or after last one)
        df['tag'] = ((df.index >= df.first_valid_index()) &
                     (df.index <= df.last_valid_index()) &
                     df.isnull().transpose().as_matrix()).transpose()

        # make another DF to hold info about each region
        nan_regs = pd.DataFrame()

        # first row of consecutive region is a True preceded by a False in tags
        nan_regs['start_idx'] = df.index[df['tag'] & ~ df['tag'].shift(1).fillna(False)]

        # last row of consecutive region is a False preceded by a True   
        nan_regs['till_idx'] = df.index[df['tag'] & ~ df['tag'].shift(-1).fillna(False)] 

        if not df['tag'].any():
            logger.info('%s : nothing to patch in this column', col_name[0:3])
            df.drop('tag', axis=1, inplace=True)
            nan_idx = pd.MultiIndex.from_arrays([[0, 0, 0, 0], ['count', 'span', 'start_idx', 'till_idx']])
            nan_list = pd.DataFrame(index=nan_idx, columns=df.columns)
        else:
            # how long is each region
            nan_regs['span'] = nan_regs['till_idx'] - nan_regs['start_idx'] + one_period
            nan_regs['count'] = (nan_regs['span'] / one_period)
            # sort the info DF to put longest missing region on top
            nan_regs = nan_regs.sort_values('count', ascending=False).reset_index(drop=True)
            
            df.drop('tag', axis=1, inplace=True)
            nan_list = nan_regs.stack().to_frame()
            nan_list.columns = df.columns

            for i, row in nan_regs.iterrows():
                j = 0
                # interpolate missing value spans up to 2 hours
                if row['span'] <= timedelta(hours=2):
                    if i + 1 == len(nan_regs):
                        logger.info('%s : \n        interpolated %s '
                                    'up-to-2-hour-spans of NaNs',
                                    col_name[0:3], i + 1 - j)
                    to_fill = slice(row['start_idx'] - one_period,
                                     row['till_idx'] + one_period)
                    df.iloc[:,0].loc[to_fill] = df.iloc[:,0].loc[to_fill].interpolate()

                # guess missing value spans longer than one hour based on other tsos
                elif col_name[1][:2] == 'DE' and col_name[2] == 'generation':
                    j += 1
#                    logger.info('guessed %s entries after %s', row['count'], row['start_idx'])
                    day_before = pd.DatetimeIndex(freq='15min',
                                                  start=row['start_idx'] - timedelta(hours=24),
                                                  end=row['start_idx'] - one_period)

                    to_fill = pd.DatetimeIndex(freq='15min',
                                                start=row['start_idx'],
                                                end=row['till_idx'])

                    # other_tsos = [c[1] for c in compact.drop(col_name, axis=1).loc[:,(col_name[0],slice(None),col_name[2])].columns.tolist()]
                    other_tsos = [tso for tso in ['DE50hertz', 'DEamprion', 'DEtennet', 'DEtransnetbw'] if tso != col_name[1]]
                    
                    # select columns with data for same technology (wind/solar) but from other TSOs
                    similar = frame.loc[:,(col_name[0],other_tsos,col_name[2])]
                    # calculate the sum using columns without NaNs the day 
                    # before or during the period to be guessed
                    similar = similar.dropna(axis=1, how='any', subset=day_before.append(to_fill)).sum(axis=1)
                    # calculate scaling factor for other TSO data
                    factor =  similar.loc[day_before].sum(axis=0) / df.loc[day_before,:].sum(axis=0)
                    
                    guess = similar.loc[to_fill] / float(factor)
                    df.iloc[:,0].loc[to_fill] = guess
                    a = float(df.iloc[:,0].loc[row['start_idx'] - one_period])
                    b = float(df.iloc[:,0].loc[row['start_idx']])
                    if a == 0:
                        deviation = '{} absolut'.format(a - b)
                    else:
                        deviation = '{:.2f} %'.format((a - b) / a * 100)
                    logger.info('%s : \n        '
                                'guessed %s entries after %s \n        '
                                'last non-missing: %s \n        '
                                'first guessed: %s \n        '
                                'deviation of first guess from last known value: %s', 
                                col_name[0:3], row['count'], row['start_idx'],
                                a, b, deviation)                  

        if len(nan_table) == 0:
            nan_table = nan_list
        else:
            nan_table = nan_table.combine_first(nan_list)

        if len(patched) == 0:
            patched = df
        else:
            patched = patched.combine_first(df)
            
    nan_table.columns.names = HEADERS
    patched.columns.names = HEADERS

    return patched, nan_table


# Patch the 15 minutes dataset and display the location of missing Data in the original data.

# In[ ]:

patched, nan_table = patcher(data_sets['15min'])
#nan_table#.loc[(slice(None),['count','start_idx']),:]


# Execute this to see whether there is still missing data. This is the case for some of the forecast columns.

# In[ ]:

patched2, nan_table2 = patcher(patched)
nan_table2.loc[(slice(None),['count','start_idx']),:]


# Execute this to see an example of where the data has been patched.

# In[ ]:

data_sets['15min'].loc['2015-10-24 23:00:00':'2015-10-25 03:00:00', 'wind']


# In[ ]:

patched.loc['2015-10-24 23:00:00':'2015-10-25 03:00:00', 'wind']


# Replace the untreated data set with the patched one.

# In[ ]:

data_sets['15min'] = patched


# ## 3.2 Aggregate German data from individual TSOs

# The wind and solar in-feed data for the 4 German balancing areas is summed up and stored in in new columns, which are then used to calculate profiles, that is, the share of wind/solar capacity producing at a given time. The column headers are created in the fashion introduced in the read script.

# In[ ]:

web = 'http://data.open-power-system-data.org/datapackage_timeseries'
for tech in ['wind', 'solar']:
    for attribute in ['generation', 'forecast']:
        sum_col = pd.Series()
        for tso in ['DE50hertz', 'DEamprion', 'DEtennet', 'DEtransnetbw']:
            try:
                add_col = data_sets['15min'][tech, tso, attribute]
                if len(sum_col) == 0:
                    sum_col = add_col
                else:
                    sum_col = sum_col + add_col.values
            except KeyError:
                pass
                
        # Create a new MultiIndex
        tuples = [(tech, 'DE', attribute, 'own calculation', web)]
        columns = pd.MultiIndex.from_tuples(tuples, names=HEADERS)
        sum_col.columns = columns
        data_sets['15min'] = data_sets['15min'].combine_first(sum_col)
        
        # Calculate the profile column
        try:
            if attribute == 'generation':
                profile_col = sum_col.values / data_sets['15min'][tech, 'DE', 'capacity']
                tuples = [(tech, 'DE', 'profile', 'own calculation', web)]
                columns = pd.MultiIndex.from_tuples(tuples, names=HEADERS)
                profile_col.columns = columns
                data_sets['15min'] = data_sets['15min'].combine_first(profile_col)
        except KeyError:
            pass  # FIXME


# New columns for the aggregated data have been added to the 15 minutes dataset.

# In[ ]:

data_sets['15min']


# ## 3.3 Create hourly data from 15' data

# The German renewables in-feed data comes in 15-minute intervals. We resample it to hourly intervals in order to match the load data from ENTSO-E.

# In[ ]:

resampled = data_sets['15min'].resample('H').mean()
try:
    data_sets['60min'] = data_sets['60min'].combine_first(resampled)
except KeyError:
    data_sets['60min'] = resampled


# New columns for the resampled data have been added to the 60 minutes dataset.

# In[ ]:

data_sets['60min']


# # 4. Create metadata

# In this part, we create the metadata that will document the data output in CSV format. The metadata we be stored in JSON format, which is very much like a python dictionary.

# ## 4.1 General metadata

# First, we define the general metadata for the timeseries datapackage

# In[ ]:

metadata = {
    'name': 'opsd-timeseries',
    'title': 'Time-series data: load, wind and solar, prices',
    'description': 'This data package contains different kinds of timeseries '
        'data relevant for power system modelling. Currently, the data '
        'includes hourly electricity consumption (load) for 36 European '
        'countries, wind and solar power generation from German transmission '
        'system operators for every quarter hour, and daily wind and solar '
        'capacity data. We use '
        'this data to calculate Germany-wide renewables in-feed and profile '
        'timeseries. We plan to include more data sources in the future. '
        'While some of the wind in-feed data dates back to '
        '2005, the full dataset is only available from 2012 onwards. The '
        'data has been downloaded from the sources, resampled and merged in '
        'a large CSV file with hourly resolution. Additionally, the data '
        'available at a higher resolution (German renewables in-feed, 15 '
        'minutes) is provided in a separate file. All data processing is '
        'conducted in python and pandas and has been documented in the '
        'Jupyter notebooks linked below.',
    'opsd-jupyter-notebook-url': 'https://github.com/Open-Power-System-Data/'
        'datapackage_timeseries/blob/master/main.ipynb',
    'version': '2016-03-30',
    'opsd-changes-to-last-version': 'Added missing data handling to patch '
        'gaps in the data from German TSOs',
    'keywords': [
        'timeseries','electricity','in-feed','capacity','renewables', 'wind',
        'solar','load','tso','europe','germany'
        ],
    'geographical-scope': 'Europe/Germany',
    'licenses': [{
        'url': 'http://example.com/license/url/here',
        'version': '1.0',
        'name': 'License Name Here',
        'id': 'license-id-from-open'
        }],
    'views': [{}],
    'sources': [{
        'name': 'See the "Source" column in the field documentation'
        }],
    'maintainers': [{
        'web': 'http://example.com/',
        'name': 'Jonathan Muehlenpfordt',
        'email': 'muehlenpfordt@neon-energie.de'
        }],
    'resources': [{ # The following is an example of how the file-specific metadata is 
        'path': 'path_to.csv', # structured. The actual metadata is created below
        'format': 'csv',
        'mediatype': 'text/csv',
        'schema': {
            'fields': [{
                'name': 'load_AT_actual',
                'description': 'Consumption in Austria in MW',
                'type': 'number',
                'source': {
                    'name': 'Example',
                    'web': 'http://www.example.com'
                    },
                'opsd-properties': {
                    'Country': 'AT',
                    'Variable': 'load',
                    }
                }]
            }
        }]
    }

indexfield = {
    'name': 'timestamp',
    'description': 'Start of timeperiod in UTC',
    'type': 'datetime',
    'format': 'YYYY-MM-DDThh:mm:ssZ'
    }

descriptions = {
    'load': 'Consumption in {geo} in MW',
    'generation': 'Actual {tech} generation in {geo} in MW',
    'forecast': '{tech} day-ahead generation forecast in {geo} in MW',
    'capacity': '{tech} capacity in {geo} in MW',
    'profile': 'Share of {tech} capacity producing in {geo}',
    'offshoreshare': '{tech} actual offshore generation in {geo} in MW'
    }


# ## 4.2 Columns-specific metadata

# For each dataset/outputfile, the metadata has an entry in the "resources" list that describes the file/dataset. The main part of each entry is the "schema" dictionary, consisting of a list of "fields", meaning the columns in the dataset. The first field is the timestamp index of the dataset. For the other fields, we iterate over the columns of the MultiIndex index of the datasets to contruct the corresponding metadata.
# 
# At the same time, a copy of the datasets is created that has a single line column index instead of the MultiIndex.

# In[ ]:

data_sets_singleindex = copy.deepcopy(data_sets)##########################
resources = []
for res_key, data_set in data_sets.items():
    columns_singleindex = [] ####################
    fields = [indexfield]
    for col in data_set.columns:
        h = {k: v for k, v in zip(HEADERS, col)}
        if len(h['country']) > 2:
            geo = h['country'] + ' control area'
        elif h['country'] == 'NI':
            geo = 'Northern Ireland'
        elif h['country'] == 'CS':
            geo = 'Serbia and Montenegro'
        else:
            geo = pycountry.countries.get(alpha2=h['country']).name

        field = {}    
        field['description'] = descriptions[h['attribute']].format(
            tech=h['variable'], geo=geo)
        field['type'] = 'number'
        field['source'] = {
            'name': h['source'],
            'web': h['web']
            }
        field['opsd-properties'] = {
            'Country': h['country'],
            'Variable': h['variable'],
            }
        components = [h['variable'], h['country']]
        if not h['variable'] == 'load':
            components.append(h['attribute'])
            field['opsd-properties']['Attribute'] = h['attribute']
        field['name'] = '_'.join(components)
        columns_singleindex.append(field['name'])
        fields.append(field)
        
    resource = {
        'path': 'timeseries' + res_key + '.csv',
        'format': 'csv',
        'mediatype': 'text/csv',
        'alternative_formats': [
        {
          'path': 'timeseries' + res_key + '.csv',
          'stacking': 'Singleindex',
          'format': 'csv'
        },
        {
          'path': 'timeseries' + res_key + '.xlsx',
          'stacking': 'Singleindex',
          'format': 'xlsx'
        },
        {
          'path': 'timeseries' + res_key + '_multiindex.xlsx',
          'stacking': 'Multiindex',
          'format': 'xlsx'
        },
        {
          'path': 'timeseries' + res_key + '_multiindex.csv',
          'stacking': 'Multiindex',
          'format': 'csv'
        },
        {
          'path': 'timeseries' + res_key + '_stacked.csv',
          'stacking': 'Stacked',
          'format': 'csv'
        }
      ],        
        'schema': {'fields': fields}
        }       
    resources.append(resource)
    data_sets_singleindex[res_key].columns = columns_singleindex ###############
    
metadata['resources'] = resources


# Execute this to write the metadata to disk

# In[ ]:

datapackage_json = json.dumps(metadata, indent=2, separators=(',', ': '))
with open('datapackage.json', 'w') as f:
    f.write(datapackage_json)


# # 5. Write the data to disk

# Finally, we want to write the data to the output files and save it in the directory of this notebook. First, we prepare different shapes of the dataset.

# In[ ]:

data_sets_multiindex = {}
data_sets_stacked = {}
for res_key in ['15min', '60min']:
    data_sets_multiindex[res_key + '_multiindex'] = data_sets[res_key]
    
    stacked = data_sets[res_key].copy()
    stacked.columns = stacked.columns.droplevel(['source', 'web'])
    stacked = stacked.transpose().stack(dropna=True).to_frame(name='data')
    data_sets_stacked[res_key + '_stacked'] = stacked


# ## 5.1 Write to SQL-database

# This file format is required for the filtering function on the OPSD website. This takes about 30 seconds to complete.

# In[ ]:

def write_sql(path):
    for res_key, data_set in data_sets_singleindex.items():
        table = 'timeseries' + res_key
        ds = data_set.copy()
        ds.index = ds.index.strftime('%Y-%m-%dT%H:%M:%SZ')
        ds.to_sql(table, sqlite3.connect(path),
                  if_exists='replace', index_label='timestamp')
    return

write_sql('data.sqlite')


# ## 5.2 Write to Excel

# This takes about 15 Minutes to complete.

# In[ ]:

def write_excel():
    for res_key, data_set in chain(data_sets_singleindex.items(),
                                   data_sets_multiindex.items()):
        f = 'timeseries' + res_key
        data_set.to_excel(f+ '.xlsx', float_format='%.2f')
write_excel()


# ## 5.3 Write to CSV

# This takes about 10 minutes to complete.

# In[ ]:

def write_csv():
    for res_key, data_set in chain(data_sets_singleindex.items(),
                                   data_sets_multiindex.items(),
                                   data_sets_stacked.items()):
        f = 'timeseries' + res_key
        data_set.to_csv(f + '.csv', float_format='%.2f',
                        date_format='%Y-%m-%dT%H:%M:%SZ')
write_csv()


# # 6. Plausibility checks

# work in progress

# In[ ]:

# pv = compact.xs(('solar'), level=('variable'), axis=1, drop_level=False)
# pv.index = pd.MultiIndex.from_arrays([pv.index.date, pv.index.time], names=['date','time'])
# pv


# In[ ]:

# pv.groupby(level='time').max()


# In[ ]:

# pv.unstack().idxmax().to_frame().unstack().transpose()

