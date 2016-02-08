
# coding: utf-8

# # 1. Open Power System Data: time series

# ## 1.1 About this notebook

# This is a python script that downloads and processes time-series data from European power systems. Data include electricity consumption (load) from ENTSO-E, wind and solar power generation from various transmission system operators, and wind and solar capacity data from different sources. Processing and data sources are documented in this notebook.
# 
# The script produces two CSV files containing all data: one on hourly resolution and one in quarter-hourly resolution. The latter containts only those parameters that are available in quarter-hourly resolution.
# 
# Download all data usually takes several hours. The two output files are about 50 MB each.
# 
# 

# ## 1.2 Data sources

# For an overview of power system data sources see our [Data Sources](http://open-power-system-data.org/data-sources) project page. 
# 
# We use **load **data from
# - [ENTSO-E](https://www.entsoe.eu/data/data-portal/consumption/Pages/default.aspx)
# 
# We use **wind and solar generation** data from
# - [50Hertz](http://www.50hertz.com/en/Grid-Data)
# - [Amprion](http://www.amprion.net/en/grid-data)
# - [TransnetBW](https://www.transnetbw.com/en/key-figures)
# - [TenneT](http://www.tennettso.de/site/en/Transparency/publications/network-figures/overview)
# 
# We use **wind and solar capacity** data from
# - ...
# 
# We use **spot price** data from
# - ...

# # Table of Contents
# * [1. Open Power System Data: time series](#1.-Open-Power-System-Data:-time-series)
# 	* [1.1 About this notebook](#1.1-About-this-notebook)
# 	* [1.2 Data sources](#1.2-Data-sources)
# * [2. Settings](#2.-Settings)
# 	* [2.1 Libraries](#2.1-Libraries)
# * [3. Downloading the data](#3.-Downloading-the-data)
# 	* [3.1 Creating a data directory](#3.1-Creating-a-data-directory)
# 	* [3.2 Parameters for individual data sources](#3.2-Parameters-for-individual-data-sources)
# 		* [3.2.1 ENTSO-E](#3.2.1-ENTSO-E)
# 		* [3.2.2 '50Hertz](#3.2.2-'50Hertz)
# 		* [3.2.3 Amprion](#3.2.3-Amprion)
# 		* [3.2.4 TransnetBW](#3.2.4-TransnetBW)
# 		* [3.2.5 TenneT](#3.2.5-TenneT)
# 	* [3.3 Creating YAML strings](#3.3-Creating-YAML-strings)
# 	* [3.4 Downloading files one by one](#3.4-Downloading-files-one-by-one)
# * [4. Reading in, processing and aggregating the data](#4.-Reading-in,-processing-and-aggregating-the-data)
# 	* [4.1 Defining individual read funtions for each data source](#4.1-Defining-individual-read-funtions-for-each-data-source)
# 		* [4.1.1 ENTSO-E](#4.1.1-ENTSO-E)
# 		* [4.1.2 '50Hertz](#4.1.2-'50Hertz)
# 		* [4.1.3 Amprion](#4.1.3-Amprion)
# 		* [4.1.4 TransnetBW](#4.1.4-TransnetBW)
# 		* [4.1.5 TenneT](#4.1.5-TenneT)
# 	* [4.2 Reading files one by one](#4.2-Reading-files-one-by-one)
# 		* [4.2.1 Creat an empty DataFrame](#4.2.1-Creat-an-empty-DataFrame)
# 		* [4.2.2 Apply the processing function one-by-one](#4.2.2-Apply-the-processing-function-one-by-one)
# 	* [4.3 Display the Dataset](#4.3-Display-the-Dataset)
# 	* [4.4 Creating German data from individual TSOs](#4.4-Creating-German-data-from-individual-TSOs)
# 	* [4.5 Create hourly data from 15' data](#4.5-Create-hourly-data-from-15'-data)
# * [5. Save csv file to disk](#5.-Save-csv-file-to-disk)
# 

# # 2. Settings

# ## 2.1 Libraries

# Loading some python libraries.

# In[ ]:

from datetime import datetime, date
import pytz
import yaml
import requests
import os
import numpy as np
import pandas as pd
import logging


# Set up a log

# In[ ]:

logger = logging.getLogger('log')
logger.setLevel('INFO')


# # 3. Downloading the data

# First, a data directory is created on your local computer. Then, download parameters for each data source are defined, including the URL. These parameters are then turned into a YAML-string. Finally, the download is executed one by one. If all data need to be downloaded, this usually takes several hours.

# ## 3.1 Creating a data directory

# This section creates a folder "downloadpath" inside the notebook's directory on the user's local computer for the downloaded data. The folder is labelled with a time stamp.

# In[ ]:

downloadpath = 'downloads1'
if not os.path.exists(downloadpath): os.makedirs(downloadpath)
archivepath = os.path.join(
    downloadpath, 'archive-' + datetime.now().strftime('%Y-%m-%d')
    )


# Do you want to save a copy of each downloaded file under the original filename? If so, Set ARCHIVE = True, if not, set ARCHIVE = False. The latter has the advantage that it's considerably faster if some files have already been downladed during a previous run, whereas the former will download every file again, because otherwise it's not possible to compare the filenames.

# In[ ]:

ARCHIVE = True


# In[ ]:

ARCHIVE = False


# ## 3.2 Parameters for individual data sources

# This section contains a python dictionary for each download source with input parameters needed to generate the URLs for the data.

# ### 3.2.1 ENTSO-E

# In[ ]:

entso = """
ENTSO-E: 
    Data_Portal: 
        url_template: https://www.entsoe.eu/fileadmin/template/other/statistical_database/excel.php
        url_params:
            pid: '136'
            opt_period: '0'
            send: send
            opt_Response: '99'
            dataindx: '0'
            opt_Month: '{u_start.month}'
            opt_Year: '{u_start.year}'
        frequency: M
        start: 2006-01-01
        end: recent
        filetype: xls
"""


# ### 3.2.2 '50Hertz

# In[ ]:

hertz = """
50Hertz: 
    wind: 
        url_template: http://ws.50hertz.com/web01/api/WindPowerForecast/DownloadFile
        url_params:
            callback: '?'
            fileName: '{u_start:%Y}.csv'
        frequency: A
        start: 2005-01-01
        end: recent
        filetype: csv
    pv: 
        url_template: http://ws.50hertz.com/web01/api/PhotovoltaicForecast/DownloadFile
        url_params:
            callback: '?'
            fileName: '{u_start:%Y}.csv'
        frequency: A
        start: 2012-01-01
        end: recent
        filetype: csv
"""


# ### 3.2.3 Amprion

# In[ ]:

amprion = """
Amprion:
    wind: 
        url_template: http://amprion.de/applications/applicationfiles/winddaten2.php
        url_params:
            mode: download
            format: csv
            start: '{u_start.day}.{u_start.month}.{u_start.year}'
            end: '{u_end.day}.{u_end.month}.{u_end.year}' # dates must not be zero-padded
        frequency: complete
        start: 2008-01-04
        end: recent
        filetype: csv
    pv: 
        url_template: http://amprion.de/applications/applicationfiles/PV_einspeisung.php
        url_params:
            mode: download
            format: csv
            start: '{u_start.day}.{u_start.month}.{u_start.year}'
            end: '{u_end.day}.{u_end.month}.{u_end.year}' # dates must not be zero-padded        
        frequency: complete
        start: 2010-01-07
        end: recent
        filetype: csv
"""


# ### 3.2.4 TransnetBW

# In[ ]:

transnetbw = """
TransnetBW: 
    wind: 
        url_template: https://www.transnetbw.de/de/kennzahlen/erneuerbare-energien/windenergie
        url_params:
            app: wind
            activeTab: csv
            view: '1'
            download: 'true'
            selectMonatDownload: '{u_transnet}'
        frequency: M
        start: 2010-01-01
        end: recent
        filetype: csv
    pv: 
        url_template: https://www.transnetbw.de/de/kennzahlen/erneuerbare-energien/fotovoltaik
        url_params:
            app: wind
            activeTab: csv
            view: '1'
            download: 'true'
            selectMonatDownload: '{u_transnet}'
        frequency: M
        start: 2011-01-01
        end: recent
        filetype: csv
"""


# ### 3.2.5 TenneT

# In[ ]:

tennet = """
TenneT: 
    wind: 
        url_template: http://www.tennettso.de/site/de/phpbridge
        url_params:
            commandpath: Tatsaechliche_und_prognostizierte_Windenergieeinspeisung/monthDataSheetCsv.php
            contenttype: text/x-csv
            querystring: monat={u_start:%Y-%m}
        frequency: M
        start: 2006-01-01
        end: recent
        filetype: csv        
    pv: 
        url_template: http://www.tennettso.de/site/de/phpbridge
        url_params:
            commandpath: Tatsaechliche_und_prognostizierte_Solarenergieeinspeisung/monthDataSheetCsv.php
            sub: total
            contenttype: text/x-csv
            querystring: monat={u_start:%Y-%m}
        frequency: M
        start: 2010-01-01
        end: recent
        filetype: csv  
"""


# ## 3.3 Creating YAML strings

# Loading the parameters for the data sources we wish to include into a [YAML](https://en.wikipedia.org/wiki/YAML)-string.

# In[ ]:

conf = yaml.load(hertz + amprion + tennet + transnetbw + entso)


# ## 3.4 Downloading files one by one

# In the following we iterate over the sources and technology (wind/solar) entries specified above and download the data for a the period given in the parameters. The filename is chosen so it reveals the files contents.
# 
# If archive is set to 'True', a copy of each file is also saved under it's original filename. Note that the original file names are often not self-explanatory (called "data" or "January").

# In[ ]:

def download(session, source, tech, s, e, **p):
    """construct URLs from template and parameters, save, and archive."""
    logger.info(
        'Attempting download of: {} {} {:%Y-%m-%d}-{:%Y-%m-%d}'.format(
            source, tech, s, e
            )
        )
    work_file = os.path.join(
        downloadpath,
        source + '_' + tech +'_' +
        s.strftime('%Y-%m-%d') + '_'+
        e.strftime('%Y-%m-%d') + '.' +
        p['filetype']
        )
    
    count = datetime.now().month - s.month + (datetime.now().year - s.year)*12
    for key, value in p['url_params'].items():
        p['url_params'][key] = value.format(
            u_start = s,
            u_end = e,
            u_transnet = count
            )
        
    if not os.path.exists(work_file) or ARCHIVE:
        resp = session.get(p['url_template'], params=p['url_params'])                
        logger.info('From URL: %s', resp.url)        
        save(resp, work_file)
        
        if ARCHIVE:        
            original_filename = resp.headers['content-disposition'].split(
                'filename=')[-1].replace('"','').replace(';','')                
            logger.info(
                'Archiving under original filename: %s', original_filename
                )
            full_archivepath = os.path.join(archivepath, source, tech)
            if not os.path.exists(full_archivepath):
                os.makedirs(full_archivepath)
            archive_file = os.path.join(full_archivepath, original_filename)
            save(resp, archive_file)
            
    else:
        logger.info('File already downloaded. Skip to next.')
        
    return


def save(resp, filepath):
    """save a file from a response-object under a given path"""
    if not os.path.exists(filepath):        
        with open(filepath, 'wb') as output_file:
            for chunk in resp.iter_content(1024):
                output_file.write(chunk)
    else:
        logger.info('File already exists. Skip to next.')
        
    return


for source, t in conf.items():
    for tech, p in t.items():
        session = requests.session()
#        p['start'] = date(2015,1,1) #uncomment this to set a different start
        if p['end'] == 'recent':
            p['end'] = date(2015,12,31)

        if p['frequency'] == 'complete':
            download(session, source, tech, p['start'], p['end'], **p)            
        else:
            starts = pd.date_range(
                start=p['start'], end=p['end'], freq=p['frequency']+'S')
            ends = pd.date_range(
                start=p['start'], end=p['end'], freq=p['frequency'])
            for start, end in zip(starts, ends):
                download(session, source, tech, start, end, **p)        
                


# # 4. Reading in, processing and aggregating the data

# We want to merge the downloadet files into one singe CSV file. 
# 
# Every data source  provides the data in a different format, using different labelling and a specific treatment of dayligh saving times. This requires custom read functionality for every source.
# 
# We first define individual read functions for each data source. Then, an empty DataFrame is created and and apply the read functions are applied one by one. In the next step, part of the data frame and some descriptive statistics are displayed for error checking. Then, data from the the four German TSOs are aggregated. In the last step, quarter-hourly data is transformed to hourly-data.

# ## 4.1 Defining individual read funtions for each data source

# ### 4.1.1 ENTSO-E

# In[ ]:

def read_entso(filepath, source, tech):
    data = pd.read_excel(
        io=filepath,
        header=9,
        skiprows=None,
        index_col=[0, 1],
        parse_cols = None #None means: parse all columns
        )
    
#    dst_transition_days = [d.date() for d in pytz.timezone(
#            'Europe/Berlin')._utc_transition_times[1:]]
    dst_transition_times = [d.replace(hour=2) for d in pytz.timezone(
            'Europe/Berlin')._utc_transition_times[1:]]
    
    # The original data has days and countries in the rows and hours in the
    # columns.  This rearranges the table, mapping hours on the rows and
    # countries on the columns.  
    data = data.stack(level=None).unstack(level='Country').reset_index()    
    # pythons DataFrame.stack() puts former columnnames in a new index object
    # named after their level.
    data.rename(columns={'level_1': 'raw_hour'}, inplace=True)
    
    # Truncate the hours column and replace letters (incating which is which
    # during fall dst-transition).  Hours are indexed 1-24 rather then 0-23,
    # so we deduct 1.
    data['hour'] = (data['raw_hour'].str[:2].str.replace(
            'A','').str.replace('B','').astype(int) - 1).astype(str)    
    data['dt_index'] = pd.to_datetime(data['Day']+' '+data['hour']+':00')
    data.set_index('dt_index', inplace=True)    
    
    # Drop 2nd occurence of 03:00 appearing in October data except for autumn
    # dst-transition
    data = data[~((data['raw_hour'] == '3B:00:00') & ~(
                data.index.isin(dst_transition_times)))]
    # Drop 03:00 for (spring) dst-transition. October data is unaffected because
    # the format is 3A:00/3B:00 
    data = data[~((data['raw_hour'] == '03:00:00') & (
                data.index.isin(dst_transition_times)))]
    
    data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')
    data.drop(['Day', 'hour', 'raw_hour'], axis=1, inplace=True)
    data.rename(columns=lambda x: 'load_' + x, inplace=True)
    data = data.replace(to_replace='n.a.', value=np.nan)
    
    return data


# ### 4.1.2 '50Hertz

# In[ ]:

def read_hertz(filepath, source, tech):
    data = pd.read_csv(
        filepath,
        sep=";",
        header=3,
        index_col='dt_index',
        names=[
            'date',
            'time',
            source + '_' + tech + '_actual'
            ],
        parse_dates={'dt_index': ['date', 'time']},
        date_parser=None,
        dayfirst=True,
        decimal=',',
        thousands='.',
        # truncate values in 'time' column after 5th character
        converters={'time': lambda x: x[:5]},
        usecols=[0, 1, 3],
    )
    
    # Until 2006 as well as  in 2015, only the wintertime October dst-transition
    # (marked by a B in the data) is reported, the summertime hour, (marked by 
    # an A) is missing in the data.      
    if pd.to_datetime(data.index.values[0]).year not in range(2007,2015):
        dst_arr = np.zeros(len(data.index), dtype=bool)
        data.index = data.index.tz_localize('Europe/Berlin', ambiguous=dst_arr)
    else:
        data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')
    
    return data            


# ### 4.1.3 Amprion

# In[ ]:

def read_amprion(filepath, source, tech):
    data = pd.read_csv(
        filepath,
        sep=";",
        header=0,
        index_col='dt_index',
        names=[
            'date',
            'time',
            source + '_' + tech + '_forecast',
            source + '_' + tech + '_actual'
            ],
        parse_dates={'dt_index' : ['date', 'time']},
        date_parser=None,
        dayfirst=True,
#        decimal=',', #shouldn't be relevant
        thousands=None,
        # Truncate values in 'time' column after 5th character.
        converters={'time': lambda x: x[:5]},
        usecols=[0, 1, 2, 3],        
    )

    index1 = data.index[data.index.year <= 2009]
    index1 = index1.tz_localize('Europe/Berlin', ambiguous='infer')        
    index2 = data.index[data.index.year > 2009]
    dst_arr = np.ones(len(index2), dtype=bool)
    index2 = index2.tz_localize('Europe/Berlin', ambiguous=dst_arr)        
    data.index = index1.append(index2)

    # dst_arr is a boolean array consisting only of "True" entries, telling 
    # python to treat the hour from 2:00 to 2:59 as summertime.  
    
    return data


# ### 4.1.4 TransnetBW

# In[ ]:

def read_transnetbw(filepath, source, tech):
    data = pd.read_csv(
        filepath,
        sep=";",
        header=0,
        index_col="dt_index",
        names=[
            'date',
            'time',
            source + '_' + tech + '_forecast',
            source + '_' + tech + '_actual'
            ],
        parse_dates={'dt_index': ['date', 'time']},
        date_parser=None,         
        dayfirst=True,
        decimal=',',
        thousands=None,
        converters=None,
        usecols=[0, 1, 4, 5],
    )
        
    data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')
    # "ambigous" refers to how the October dst-transition hour is handled.  
    # ‘infer’ will attempt to infer dst-transition hours based on order.  
        
    return data


# ### 4.1.5 TenneT

# In[ ]:

def read_tennet(filepath, source, tech):
    data = pd.read_csv(
        filepath,
        sep=";",
        encoding='latin_1',
        header=3,
        index_col=None,
        names=[
            'date',
            'position',
            source + '_' + tech + '_forecast',
            source + '_' + tech + '_actual',
            source + '_' + tech + '_offshore_share'
            ],
        parse_dates=False,
        date_parser=None,
        dayfirst=True,
#       decimal=',', #shouldn't be relevant
        thousands=None,
        converters=None,          
        usecols=[0, 1, 2, 3, 4],
    )

    data['date'].fillna(method='ffill', limit = 100, inplace=True)

    # The Tennet Data doesn't feature a timestamp column. Instead, the 
    # quarter-hourly data entries for each day are numbered their position,
    # creating an index ranging from 1 to 96 on normal days. This index can be
    # used to compute a timestamp.  However, the behaviour for DST switch dates
    # needs to be specified seperately as follows:

    for i in range(len(data.index)):
        # On the day in March when summertime begins, shift the data forward by
        # 1 hour, beginning with the 9th quarter-hour, so the index runs again
        # up to 96
        if (data['position'][i] == 92 and (
                (i == len(data.index)-1) or
                (data['position'][i + 1] == 1)
                )
           ):
            slicer = data[
                (data['date'] == data['date'][i]) &
                (data['position'] >= 9)
                ].index
            data.loc[slicer, 'position'] = data['position'] + 4

        if data['position'][i] > 96: # True when summertime ends in October
            logger.info('%s th quarter-hour at %s, position %s',data[
                    'position'][i], data.ix[i,'date'], (i))  

            # Instead of having the quarter-hours' index run up to 100, we want 
            # to have it set back by 1 hour beginning from the 13th
            # quarter-hour, ending at 96
            if (data['position'][i] == 100 and not
                    (data['position'] == 101).any()):                    
                slicer = data[
                    (data['date'] == data['date'][i]) &
                    (data['position'] >= 13)
                    ].index
                data.loc[slicer, 'position'] = data['position'] - 4                     

            # In 2011 and 2012, there are 101 qaurter hours on the day the 
            # summertime ends, so 1 too many.  From looking at the data, we
            # inferred that the 13'th quarter hour is the culprit, so we drop
            # that.  The following entries for that day need to be shifted.
            elif data['position'][i] == 101: 
                data = data[~(
                    (data['date'] == data['date'][i]) &
                    (data['position'] == 13)
                    )]
                slicer = data[
                    (data['date'] == data['date'][i]) &
                    (data['position'] >= 13)
                    ].index
                data.loc[slicer, 'position'] = data['position'] - 5         

    # On 2012-03-25, there are 94 entries, where entries 8 and 10 are probably
    # wrong.
    if data['date'][0] == '2012-03-01':
        data = data[~(
            (data['date'] == '2012-03-25') & (
                (data['position'] == 8) |
                (data['position'] == 10)
                )
            )]
        slicer = data[
            (data['date'] == '2012-03-25') & 
            (data['position'] >= 9)
            ].index
        data.loc[slicer, 'position'] = [8] + list(range(13, 97))        

    # On 2012-09-27, there are 97 entries. Probably, just the 97th entry is wrong
    if data['date'][0] == '2012-09-01':
        data = data[~(
            (data['date'] == '2012-09-27') &
            (data['position'] == 97)
            )]          

    # Here we compute the timestamp from the position and generate the
    # datetime-index
    data['hour'] = (np.trunc((data['position']-1)/4)).astype(int).astype(str)
    data['minute'] = (((data['position']-1)%4)*15).astype(int).astype(str)
    data['dt_index'] = pd.to_datetime(
        data['date'] + ' ' +
        data['hour'] + ':' +
        data['minute'],
        dayfirst = True
        )
    data.set_index('dt_index',inplace=True)

    # In the years 2006, 2008, and 2009, the dst-transition hour in March
    # appears as empty rows in the data.  We delete it from the set in order to
    # make the timezone localization work.  
    for crucial_date in pd.to_datetime([
            '2006-03-26',
            '2008-03-30',
            '2009-03-29'
            ]).date:
        if data.index[0].year == crucial_date.year:
            data = data[~(
                (data.index.date == crucial_date) &
                (data.index.hour == 2)
                )]

    data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')

#    data = data.drop(['position', 'date', 'hour', 'minute'], axis=1)
    if tech == 'wind':
        data = data[[
            source + '_' + tech + '_forecast',
            source + '_' + tech + '_actual',
            source + '_' + tech + '_offshore_share'
            ]]
    if tech == 'pv':
        data = data[[
            source + '_' + tech + '_forecast',
            source + '_' + tech + '_actual',
            ]]

    return data


# ## 4.2 Reading files one by one

# ### 4.2.1 Creat an empty DataFrame

# Create an empty DataFrame / reset the DataFrame

# In[ ]:

resultDataSet = pd.DataFrame()


# ### 4.2.2 Apply the processing function one-by-one

# For each source/TSO and technology specified in the conf dict, this section finds all the downloaded files in the downloads folder and then calls the matching readData function.
# The datasets returned by the read function are then merged into one large dataset.

# In[ ]:

conf = yaml.load(hertz + amprion + tennet + transnetbw + entso)


# In[ ]:

conf = yaml.load(hertz + entso)


# In[ ]:

for source, t in conf.items():
    for tech, param in t.items():
        for filename in os.listdir(downloadpath):
            if source in filename and tech in filename:
                logger.info('reading %s', filename)
                if os.path.getsize(os.path.join(downloadpath, filename)) < 128:
                    logger.info('file is smaller than 128 Byte,' +
                                'which means it is probably empty')
                else:
                    filepath = os.path.join(downloadpath, filename)
                    if source == 'ENTSO-E':
                        dataToAdd = read_entso(filepath, source, tech)
                    elif source == 'Svenska_Kraftnaet':
                        dataToAdd = read_svenskakraftnaet(filepath, source, tech)
                    elif source == '50Hertz':
                        dataToAdd = read_hertz(filepath, source, tech)
                    elif source == 'Amprion':
                        dataToAdd = read_amprion(filepath, source, tech)
                    elif source == 'TenneT':
                        dataToAdd = read_tennet(filepath, source, tech)
                    elif source == 'TransnetBW':
                        dataToAdd = read_transnetbw(filepath, source, tech)

                    resultDataSet = resultDataSet.combine_first(dataToAdd)
#                    resultDataSet.update(dataToAdd)


# ## 4.3 Display the Dataset

# This section can be executed to display a preview of the merged dataset.

# In[ ]:

resultDataSet.head()


# In[ ]:

resultDataSet.describe()


# In[ ]:

resultDataSet.info()


# ## 4.4 Creating German data from individual TSOs

# The in-feed data for the 4 German controll areas is summed up.

# In[ ]:

resultDataSet['wind_DE'] = (
    resultDataSet['50Hertz_wind_actual'] +
    resultDataSet['Amprion_wind_actual'] +
    resultDataSet['TransnetBW_wind_actual'] +
    resultDataSet['TenneT_wind_actual']
    )    
resultDataSet['pv_DE'] = (
    resultDataSet['50Hertz_pv_actual'] +
    resultDataSet['Amprion_pv_actual'] +
    resultDataSet['TransnetBW_pv_actual'] +
    resultDataSet['TenneT_pv_actual']
    )


# ## 4.5 Create hourly data from 15' data

# Most of the renewables in-feed data comes in 15-minute intervals. We resample it to hourly intervals in order to match the load data from ENTSO-E.

# In[ ]:

DataSet60 = resultDataSet.resample('H', how='mean')


# The data that is available in 15 minute resolution extracted to be saved separately

# In[ ]:

DataSet15 = pd.DataFrame()
for column in resultDataSet.columns:
    if 'load' not in column:
        DataSet15[column] = resultDataSet[column]


# # 5. Save csv file to disk

# Finally, we write the data to CSV format and save it in the directory of this notebook. Two files are created: one in hourly granularity called "timeseries60.csv" (including all data); and one in quarter-hourly granularity called "timeseries15.csv" (including only data avaiable at this resultion).

# In[ ]:

DataSet60.to_csv('timeseries60.csv', sep=',', float_format='%.2f', decimal='.', date_format='%Y-%m-%dT%H:%M:%S%z')


# In[ ]:

DataSet15.to_csv('timeseries15.csv', sep=',', float_format='%.2f', decimal='.', date_format='%Y-%m-%dT%H:%M:%S%z')

