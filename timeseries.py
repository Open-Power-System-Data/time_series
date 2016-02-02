
# coding: utf-8

# # Open Power System Data
# ## Timeseries data
# This is a python script that downloads and processes renewables in-feed data from the German TSOs Amprion and TransnetBW.
# The output is a CSV file containing all the data.

# # Table of Contents
# * [Settings](#Settings)
# 	* [Libraries](#Libraries)
# * [Downloading the data](#Downloading-the-data)
# 	* [Parameters for download sources](#Parameters-for-download-sources)
# 		* [ENTSO-E](#ENTSO-E)
# 		* [50Hertz](#50Hertz)
# 		* [Amprion](#Amprion)
# 		* [TransnetBW](#TransnetBW)
# 		* [TenneT](#TenneT)
# 	* [Downloading files one by one](#Downloading-files-one-by-one)
# * [Processing the data](#Processing-the-data)
# 	* [Defining individual read funtions for each data source](#Defining-individual-read-funtions-for-each-data-source)
# 		* [ENTSO-E](#ENTSO-E)
# 		* [50Hertz](#50Hertz)
# 		* [Amprion](#Amprion)
# 		* [TransnetBW](#TransnetBW)
# 		* [TenneT](#TenneT)
# 	* [Reading files one by one](#Reading-files-one-by-one)
# 	* [Processing the data](#Processing-the-data)
# 	* [Display the Dataset](#Display-the-Dataset)
# * [Save csv file to disk](#Save-csv-file-to-disk)
# 

# # Settings

# ## Libraries

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
logger = logging.getLogger('log')
logger.setLevel('INFO')


# # Downloading the data

# ## Parameters for download sources

# This section contains a python dictionary for each download source with input parameters needed to generate the URLs for the data.

# ### ENTSO-E

# In[ ]:

ENTSO = """
    ENTSO-E: 
        Data_Portal: 
            url_template: https://www.entsoe.eu/fileadmin/template/other/statistical_database/excel.php
            url_params:
                pid: 136
                opt_period: 0
                send: send
                opt_Response: 99
                dataindx: 0
            url_dates:
                opt_Month: '{u_start.month}'
                opt_Year: '{u_start.year}'
            frequency: M
            start: 2006-01-01
            end: recent
            filetype: xls
"""


# ### 50Hertz

# In[ ]:

Hertz = """
    50Hertz: 
        wind: 
            url_template: http://ws.50hertz.com/web01/api/WindPowerForecast/DownloadFile
            url_params:
                callback: '?'
            url_dates:
                fileName: '{u_start:%Y}.csv'
            frequency: A
            start: 2005-01-01
            end: recent
            filetype: csv
        pv: 
            url_template: http://ws.50hertz.com/web01/api/PhotovoltaicForecast/DownloadFile
            url_params:
                callback: '?'
            url_dates:
                fileName: '{u_start:%Y}.csv'
            frequency: A
            start: 2012-01-01
            end: recent
            filetype: csv
"""


# ### Amprion

# In[ ]:

Amprion = """
    Amprion:
        wind: 
            url_template: http://amprion.de/applications/applicationfiles/winddaten2.php
            url_params:
                mode: download
                format: csv
            url_dates:
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
            url_dates:
                start: '{u_start.day}.{u_start.month}.{u_start.year}'
                end: '{u_end.day}.{u_end.month}.{u_end.year}' # dates must not be zero-padded        
            frequency: complete
            start: 2010-01-07
            end: recent
            filetype: csv
"""


# ### TransnetBW

# In[ ]:

TransnetBW = """
    TransnetBW: 
        wind: 
            url_template: https://www.transnetbw.de/de/kennzahlen/erneuerbare-energien/windenergie
            url_params:
                app: wind
                activeTab: csv
                view: 1
                download: true
            url_dates:
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
                view: 1
                download: true
            url_dates:
                selectMonatDownload: '{u_transnet}'
            frequency: M
            start: 2011-01-01
            end: recent
            filetype: csv
"""


# ### TenneT

# In[ ]:

TenneT = """
    TenneT: 
        wind: 
            url_template: http://www.tennettso.de/site/de/phpbridge
            url_params:
                commandpath: Tatsaechliche_und_prognostizierte_Windenergieeinspeisung/monthDataSheetCsv.php
                contenttype: text/x-csv
            url_dates:
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
            url_dates:
                 querystring: monat={u_start:%Y-%m}
            frequency: M
            start: 2010-01-01
            end: recent
            filetype: csv  
"""


# Loading the parameters for the data sources we wish to include into a YAML-string.

# In[ ]:

conf = yaml.load(Hertz+Amprion+TenneT+TransnetBW+ENTSO)


# ## Downloading files one by one

# This section creates folders inside the notebook's directory on the users computer for the downloaded data.

# In[ ]:

downloadpath = 'downloads1'
if not os.path.exists(downloadpath): os.makedirs(downloadpath)
archivepath = os.path.join(downloadpath, 'archive-'+datetime.now().strftime('%Y-%m-%d'))
archive = True


# Here we iterate over the sources and technology (wind/solar) entries specified above and download the data for a the period given in the parameters. The filename is chosen so it reveals the files contents
# If archive is set to 'True', a copy of each file is also saved under it's original filename

# In[ ]:

def download(session, source, tech, s, e, **p):
    """construct URLs from template and parameters, save, and archive."""
    logger.info('Attempting download of: {} {} {:%Y-%m-%d}-{:%Y-%m-%d}'.format(source, tech, s, e))
    work_file = os.path.join(
        downloadpath, source+'_'+tech+'_'+s.strftime('%Y-%m-%d')+'_'+e.strftime('%Y-%m-%d')+'.'+p['filetype']
    )        
    count = datetime.now().month - s.month + (datetime.now().year - s.year) * 12
    for date_key, date_value in p['url_dates'].items():
        p['url_params'][date_key] = date_value.format(u_start = s, u_end = e, u_transnet = count)

    if not os.path.exists(work_file) or archive == True:
        resp = session.get(p['url_template'], params=p['url_params'])                
        logger.info('From URL: %s', resp.url)        
        save(resp, work_file)
        
        original_filename = resp.headers['content-disposition'].split('filename=')[-1].replace('"','').replace(';','')                
        logger.info('Archiving under original filename: %s', original_filename)
        
        full_archivepath = os.path.join(archivepath, source, tech)
        if not os.path.exists(full_archivepath): os.makedirs(full_archivepath)
        archive_file = os.path.join(full_archivepath,original_filename)
        save(resp, archive_file)
    else: logger.info('File already downloaded. Skip to next.')        
    return

def save(resp, filepath):
    """save a file from a response-object under a given path"""
    if not os.path.exists(filepath):        
        with open(filepath, 'wb') as output_file:
            for chunk in resp.iter_content(1024):
                output_file.write(chunk)
    else: logger.info('File already exists. Skip to next.')
    return

for source, t in conf.items():
    for tech, p in t.items():
        session = requests.session()
#        p['start'] = date(2015,1,1)
        if p['end'] == 'recent':
            p['end'] = date(2015,12,31)

        if p['frequency'] == 'complete':
            download(session, source, tech, p['start'], p['end'], **p)            
        else:
            starts = pd.date_range(start=p['start'], end=p['end'], freq=p['frequency']+'S')
            ends = pd.date_range(start=p['start'], end=p['end'], freq=p['frequency'])
            for start, end in zip(starts, ends):
                download(session, source, tech, start, end, **p)        
                


# # Processing the data

# We want to merge the downloadet files into one big CSV file. Since every TSO provides the data in a different format, this requires custom read functionality for every source.

# ## Defining individual read funtions for each data source

# ### ENTSO-E

# In[ ]:

def read_ENTSO(filePath, source, tech):
    data = pd.read_excel(
        io = filePath,
        header=9,
#        skiprows = None,
        index_col = [0,1],
#        parse_cols = None #None means: parse all columns
        )
    
#    dst_transition_days = [d.date() for d in pytz.timezone('Europe/Berlin')._utc_transition_times[1:]]
    dst_transition_times = [d.replace(hour=2) for d in pytz.timezone('Europe/Berlin')._utc_transition_times[1:]]
    
    #the original data has days and countries in the rows and hours in the columns.
    #this rearranges the table, mapping hours on the rows and countries on the columns 
    data = data.stack(level=None).unstack(level='Country').reset_index()    
    #pythons DataFrame.stack() puts former columnnames in a new index object named after their level
    data.rename(columns={'level_1': 'raw_hour'}, inplace=True)
    
    #truncate the hours column and replace letters (incating which is which during fall dst-transition)
    #hours are indexed 1-24 rather then 0-23, so we deduct 1
    data['hour'] = (data['raw_hour'].str[:2].str.replace('A','').str.replace('B','').astype(int) - 1).astype(str)    
    data['dt_index'] = pd.to_datetime(data['Day']+' '+data['hour']+':00')
    data.set_index('dt_index', inplace=True)    
    
    # drop 2nd occurence of 03:00 appearing in October data except for autumn dst-transition
    data = data[~((data['raw_hour'] == '3B:00:00') & ~(data.index.isin(dst_transition_times)))]
    #drop 03:00 for (spring) dst-transition. October data is unaffected because the format is 3A:00/3B:00 
    data = data[~((data['raw_hour'] == '03:00:00') & (data.index.isin(dst_transition_times)))]
    
    data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')
    data.drop(['Day', 'hour', 'raw_hour'], axis=1, inplace = True)
    data.rename(columns=lambda x: 'load_'+x, inplace=True)
    data = data.replace(to_replace='n.a.', value=np.nan)
    return data


# ### 50Hertz

# In[ ]:

def read_50Hertz(filePath, source, tech):
    data = pd.read_csv(
        filePath,
        sep = ";",
        header = 3,
        index_col = 'dt_index',
        names = ['input_date', 'input_time', source+'_'+tech+'_actual'],
        parse_dates = {'dt_index' : ['input_date', 'input_time']},
        date_parser = None,
        dayfirst = True,
        decimal = ',',
        thousands = '.',
        converters = {'input_time' : lambda x: x[:5]}, # truncate values in 'input_time' column after 5th character
        usecols = [0,1,3],
#        engine = 'c', # only the C-engine allows to set the dtype parameter
#        dtype = 'str', # python uses float as the datatype, this sometimes causes changes in the last decimal place
    )
    
    if pd.to_datetime(data.index.values[0]).year not in range(2007,2015):
    # Until 2006 as well as  in 2015, only the wintertime October dst-transition (marked by a B in the data) is reported,
    # the summertime hour, (marked by an A) is missing in the data 
        dst_arr = np.zeros(len(data.index), dtype=bool)
        data.index = data.index.tz_localize('Europe/Berlin', ambiguous=dst_arr)
    else:
        data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')
    
    return data            


# ### Amprion

# In[ ]:

def read_Amprion(filePath, source, tech):
    data = pd.read_csv(
        filePath,
        sep = ";",
        header = 0,
        index_col = 'dt_index',
        names = ['input_date', 'input_time', source+'_'+tech+'_forecast', source+'_'+tech+'_actual'],
        parse_dates = {'dt_index' : ['input_date', 'input_time']},
        date_parser = None,
        dayfirst = True,
#        decimal = ',', #shouldn't be relevant
        thousands = None,
        converters = {'input_time' : lambda x: x[:5]}, # truncate values in 'input_time' column after 5th character
        usecols = [0,1,2,3],        
    )

    index1 = data.index[data.index.year <= 2009]
    index1 = index1.tz_localize('Europe/Berlin', ambiguous='infer')        
    index2 = data.index[data.index.year > 2009]
    dst_arr = np.ones(len(index2), dtype=bool)
    index2 = index2.tz_localize('Europe/Berlin', ambiguous=dst_arr)        
    data.index = index1.append(index2)

    # dst_arr is a boolean array consisting only of "True" entries, 
    # telling python to treat the hour from 2:00 to 2:59 as summertime
    
    return data


# ### TransnetBW

# In[ ]:

def read_TransnetBW(filePath, source, tech):
    data = pd.read_csv(
        filePath,
        sep = ";",
        header = 0,
        index_col = "dt_index",
        names = ['input_date', 'input_time', source+'_'+tech+'_forecast', source+'_'+tech+'_actual'],
        parse_dates = {'dt_index' : ['input_date', 'input_time',]},
        date_parser = None,         
        dayfirst = True,
        decimal = ',',
        thousands = None,
        converters = None,
        usecols = [0,1,4,5],
    )
        
    data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')
    # ambigous refers to how the October dst-transition hour is handled.
    # ‘infer’ will attempt to infer dst-transition hours based on order
        
    return data  


# ### TenneT

# In[ ]:

def read_TenneT(filePath, source, tech):
    data = pd.read_csv(
        filePath,
        sep = ";",
        encoding = 'latin_1',
        header = 3,
        index_col = None,
        names=['input_date', 'position', source+'_'+tech+'_forecast', source+'_'+tech+'_actual', source+'_'+tech+'_offshore_share'],
        parse_dates = False,
        date_parser = None,
        dayfirst = True,
#       decimal = ',', #shouldn't be relevant
        thousands = None,
        converters = None,          
        usecols = [0,1,2,3,4],
    )

    data['input_date'].fillna(method='ffill', limit = 100, inplace=True)

    # The Tennet Data doesn't feature a timestamp column. Instead, the quarter-hourly data entries for each day are numbered
    # by their position, creating an index ranging from 1 to 96 on normal days. This index can be used to compute a timestamp.
    # However, the behaviour for DST switch dates needs to be specified seperately as follows:

    for i in range(len(data.index)):
        # On the day in March when summertime begins, shift the data forward by 1 hour,
        # beginning with the 9th quarter-hour, so the index runs again up to 96
        if data['position'][i] == 92 and ((i == len(data.index)-1) or (data['position'][i+1] == 1)):
            slicer = data[(data['input_date'] == data['input_date'][i]) & (data['position'] >= 9)].index
            data.loc[slicer, 'position'] = data['position'] + 4

        if data['position'][i] > 96: # True when summertime ends in October
            logger.info('%s th quarter-hour at %s, position %s',data['position'][i], data.ix[i,'input_date'], (i))  

            # Instead of having the quarter-hours' index run up to 100, we want to have
            # it set back by 1 hour beginning from the 13th quarter-hour, ending at 96
            if data['position'][i] == 100 and not (data['position'] == 101).any():
                slicer = data[(data['input_date'] == data['input_date'][i]) & (data['position'] >= 13)].index
                data.loc[slicer, 'position'] = data['position'] - 4                     

            # in 2011 and 2012, there are 101 qaurter hours on the day the summertime ends, so 1 too many.
            # From looking at the data, we inferred that the 13'th quarter hour is the culprit, so we drop that.
            # The following entries for that day need to be shifted 
            elif data['position'][i] == 101: 
                data = data[~((data['input_date'] == data['input_date'][i]) & (data['position'] == 13))]
                slicer = data[(data['input_date'] == data['input_date'][i]) & (data['position'] >= 13)].index
                data.loc[slicer, 'position'] = data['position'] - 5         

    # On 2012-03-25, there are 94 entries, where entries 8 and 10 are probably wrong
    if data['input_date'][0] == '2012-03-01':
        data = data[~((data['input_date'] == '2012-03-25') & ((data['position'] == 8) | (data['position'] == 10)))]
        slicer = data[(data['input_date'] == '2012-03-25') & (data['position'] >= 9)].index
        data.loc[slicer, 'position'] = [8] + list(range(13, 97))        

    # On 2012-09-27, there are 97 entries. Probably, just the 97th entry is wrong
    if data['input_date'][0] == '2012-09-01':
        data = data[~((data['input_date'] == '2012-09-27') & (data['position'] == 97))]          

    # Here we compute the timestamp from the position and generate the datetime-index
    data['hour'] = (np.trunc((data['position']-1)/4)).astype(int).astype(str)
    data['minute'] = (((data['position']-1)%4)*15).astype(int).astype(str)
    data['dt_index'] = pd.to_datetime(data['input_date']+' '+data['hour']+':'+data['minute'], dayfirst = True)
    data.set_index('dt_index',inplace=True)

    # In the years 2006, 2008, and 2009, the dst-transition hour in March appears as empty rows in the data.
    # We delete it from the set in order to make the timezone localization work
    for crucial_date in pd.to_datetime(['2006-03-26','2008-03-30','2009-03-29']).date:
        if data.index[0].year == crucial_date.year:
            data = data[~((data.index.date == crucial_date) & (data.index.hour == 2))]

    data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')

#    data = data.drop(['position', 'input_date', 'hour', 'minute'], axis=1)
    if tech == 'wind':
        data = data[[source+'_'+tech+'_forecast', source+'_'+tech+'_actual', source+'_'+tech+'_offshore_share']]
    if tech == 'pv':
        data = data[[source+'_'+tech+'_forecast', source+'_'+tech+'_actual',]]

    return data


# ## Reading files one by one

# Create an empty DataFrame / reset the DataFrame

# In[ ]:

resultDataSet = pd.DataFrame()


# For each source/TSO and technology specified in the conf dict, this section finds all the downloaded files in the downloads folder and then calls the matching readData function.
# The datasets returned by the read function are then merged into one large dataset.

# In[ ]:

conf = yaml.load(Hertz+Amprion+TenneT+TransnetBW+ENTSO)


# In[ ]:

for source, tech in conf.items():
    for tech, param in tech.items():
        for filename in os.listdir(downloadpath):
            if source in filename and tech in filename:
                logger.info('reading %s', filename)
                if os.path.getsize(os.path.join(downloadpath, filename)) < 128:
                    logger.info("file is smaller than 128 Byte, which means it's probably empty")
                else:                    
                    if source == 'ENTSO-E':
                        dataToAdd = read_ENTSO(os.path.join(downloadpath, filename), source, tech)
                    elif source == 'Svenska_Kraftnaet':
                        dataToAdd = read_Svenska_Kraftnaet(os.path.join(downloadpath, filename), source, tech)
                    elif source == '50Hertz':
                        dataToAdd = read_50Hertz(os.path.join(downloadpath, filename), source, tech)
                    elif source == 'Amprion':
                        dataToAdd = read_Amprion(os.path.join(downloadpath, filename), source, tech)
                    elif source == 'TenneT':
                        dataToAdd = read_TenneT(os.path.join(downloadpath, filename), source, tech)
                    elif source == 'TransnetBW':
                        dataToAdd = read_TransnetBW(os.path.join(downloadpath, filename), source, tech)

                    resultDataSet = resultDataSet.combine_first(dataToAdd)
#                    resultDataSet.update(dataToAdd)


# ## Processing the data

# Most of the renewables in-feed data comes in 15-minute intervals. We resample it to hourly intervals in order to match the load data from ENTSO-E.

# In[ ]:

resampledDataSet = resultDataSet.resample('H', how='mean')


# The in-feed data for the 4 German controll areas is summed up.

# In[ ]:

resampledDataSet['wind_DE'] = (
    resampledDataSet['50Hertz_wind_actual'] +
    resampledDataSet['Amprion_wind_actual'] +
    resampledDataSet['TransnetBW_wind_actual'] +
    resampledDataSet['TenneT_wind_actual']
    )    
resampledDataSet['pv_DE'] = (
    resampledDataSet['50Hertz_pv_actual'] +
    resampledDataSet['Amprion_pv_actual'] +
    resampledDataSet['TransnetBW_pv_actual'] +
    resampledDataSet['TenneT_pv_actual']
    )


# ## Display the Dataset

# This section can be executed to display a preview of the merged dataset.

# In[ ]:

resampledDataSet.head()


# # Save csv file to disk

# Finally, we write the data to CSV format and save it in the directory of this notebook

# In[ ]:

resampledDataSet.to_csv('output1.csv', sep=';', float_format='%.2f', decimal=',', date_format='%Y-%m-%dT%H:%M:%S%z')

