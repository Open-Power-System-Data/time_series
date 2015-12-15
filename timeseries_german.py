
# coding: utf-8

# # Open Power System Data

# ## Timeseries data

# This is a python script that downloads and processes renewables in-feed data from the German TSOs Amprion and TransnetBW.
# The Ouput is one CSV file containing all the data.

# ## Table of Contents
# * [1. Settings](#1.-Settings)
# 	* [1.1 Libraries](#1.1-Libraries)
# 	* [1.2 Folder path](#1.2-Folder-path)
# 	* [1.3 Parameters for download sources](#1.3-Parameters-for-download-sources)
# * [2. Download](#2.-Download)
# 	* [2.1 Download funtions](#2.1-Download-funtions)
# 	* [2.2 Download loop](#2.2-Download-loop)
# * [3. Process the data](#3.-Process-the-data)
# 	* [3.1 Read function](#3.1-Read-function)
# 	* [3.2 reading files one by one](#3.2-reading-files-one-by-one)
# 	* [3.3 Display the Dataset](#3.3-Display-the-Dataset)
# * [4. Save csv file to disk](#4.-Save-csv-file-to-disk)
# * [Notes](#Notes)
# 

# # 1. Settings

# ## 1.1 Libraries

# Loading some python libraries

# In[ ]:

import yaml
import requests
import logging
logger = logging.getLogger('log')
logger.setLevel('INFO')
import os
from dateutil.rrule import *
from dateutil.relativedelta import *
from datetime import datetime
import numpy as np
import pandas as pd


# ## 1.2 Folder path

# This section creates folders inside the notebook's directory on the users computer for the downloaded data and the outputfiles.

# In[ ]:

downloadpath = 'downloads/'
outputpath = 'output/'
outputfile = 'timeseries_german.csv'
if not os.path.exists(downloadpath): os.makedirs(downloadpath)
if not os.path.exists(outputpath): os.makedirs(outputpath)


# ## 1.3 Parameters for download sources

# This is a python dictionary containing input parameters needed to generate the URLs belonging to the data sources

# In[ ]:

conf = """
    50hertz: 
        wind: 
            url_template: http://ws.50hertz.com/web01/api/WindPowerForecast/DownloadFile?fileName={u_start:%Y}.csv&callback=?
            bundle: YEARLY
            start: 2005-01-01
            end: recent
            filetype: csv
        pv: 
            url_template: http://ws.50hertz.com/web01/api/PhotovoltaicForecast/DownloadFile?fileName={u_start:%Y}.csv&callback=?
            bundle: YEARLY
            start: 2012-01-01
            end: recent
            filetype: csv
    Amprion:
        wind: 
            url_template: http://amprion.de/applications/applicationfiles/winddaten2.php?mode=download&format=csv&start={u_start.day}.{u_start.month}.{u_start.year}&end={u_end.day}.{u_end.month}.{u_end.year}
            bundle: complete
            start: 2008-01-04
            end: recent
            filetype: csv
        pv: 
            url_template: http://amprion.de/applications/applicationfiles/PV_einspeisung.php?mode=download&format=csv&start={u_start.day}.{u_start.month}.{u_start.year}&end={u_end.day}.{u_end.month}.{u_end.year}
            bundle: complete
            start: 2010-01-07
            end: recent
            filetype: csv
    TransnetBW: 
        wind: 
            url_template: https://www.transnetbw.de/de/kennzahlen/erneuerbare-energien/windenergie?app=wind&activeTab=csv&selectMonatDownload={month}&view=1&download=true
            bundle: special
            start: 2010-01-01
            end: recent
            filetype: csv
        pv: 
            url_template: https://www.transnetbw.de/de/kennzahlen/erneuerbare-energien/fotovoltaik?app=solar&activeTab=csv&selectMonatDownload={month}&view=1&download=true
            bundle: special
            start: 2011-01-01
            end: recent
            filetype: csv
    TenneT: 
        wind: 
            url_template: http://www.tennettso.de/site/de/phpbridge?commandpath=Tatsaechliche_und_prognostizierte_Windenergieeinspeisung%2FmonthDataSheetCsv.php&querystring=monat%3D{u_start:%Y-%m}&contenttype=text%2Fx-csv
            bundle: MONTHLY
            start: 2006-01-01
            end: recent
            filetype: csv        
        pv: 
            url_template: http://www.tennettso.de/site/de/phpbridge?commandpath=Tatsaechliche_und_prognostizierte_Solarenergieeinspeisung%2FmonthDataSheetCsv.php&sub=total&querystring=monat%3D{u_start:%Y-%m}&contenttype=text%2Fx-csv
            bundle: MONTHLY
            start: 2010-01-01
            end: recent
            filetype: csv  
"""
conf = yaml.load(conf)


# In[ ]:

conf = """
    TenneT: 
        wind: 
            url_template: http://www.tennettso.de/site/de/phpbridge?commandpath=Tatsaechliche_und_prognostizierte_Windenergieeinspeisung%2FmonthDataSheetCsv.php&querystring=monat%3D{u_start:%Y-%m}&contenttype=text%2Fx-csv
            bundle: MONTHLY
            start: 2006-01-01
            end: recent
            filetype: csv        
        pv: 
            url_template: http://www.tennettso.de/site/de/phpbridge?commandpath=Tatsaechliche_und_prognostizierte_Solarenergieeinspeisung%2FmonthDataSheetCsv.php&sub=total&querystring=monat%3D{u_start:%Y-%m}&contenttype=text%2Fx-csv
            bundle: MONTHLY
            start: 2010-01-01
            end: recent
            filetype: csv   
"""
conf = yaml.load(conf)


# # 2. Download

# ## 2.1 Download funtions

# In this section we define some functions that generate URLS from parameters

# In[ ]:

def make_url(url_template, filetype, source, tech, start, end):
    """construct URLs from a template, filling in start- and enddates and call download funtion."""    
    filename = source+'_'+tech+'_'+start.strftime('%Y-%m-%d')+'_'+end.strftime('%Y-%m-%d')
    full_url = url_template.format(u_start = start, u_end = end)
    download(full_url, filename, filetype)


# In[ ]:

def make_url_TransnetBW(url_template, filetype, count, source, tech):
    """construct URLs from a template, filling in counter and call download funtion."""   
    filename = source+'_'+tech+'_'+str(count)
    full_url = url_template.format(month = count)
    download(full_url, filename, filetype)


# This function does the actual download

# In[ ]:

def download(full_url, filename, filetype):
    """download and save file from URL and log original filename."""    
    logger.info('Attempting download of:')
    logger.info(filename)
    logger.info('From URL:')    
    logger.info(full_url)
    full_filename = downloadpath+filename+'.'+filetype
    if os.path.exists(full_filename):
        logger.info('Filename already exists. Skip to next.')
    else:
        resp = requests.get(full_url, stream = True)
        original_filename = resp.headers['content-disposition'].split('filename=')[-1]
        logger.info('original_filename:')
        logger.info(original_filename)
        with open(full_filename, 'wb') as output_file:
            for chunk in resp.iter_content(1024):
                output_file.write(chunk)


# ## 2.2 Download loop

# Here we iterate over the sources and technology (wind/solar) entries specified above and download the data for a specified period, in this case the year 2014.

# In[ ]:

rules = {'YEARLY': YEARLY,
         'MONTHLY': MONTHLY,
         'DAILY': DAILY}


# In[ ]:

for source, tech in conf.items():
    for tech, param in tech.items():
        if source == 'TransnetBW': # TransnetBW just counts the months backwards, this requires a different approach
            for count in range(12,24): # iterate over range from 12 to 23, this are the months of 2014
                make_url_TransnetBW(param['url_template'], param['filetype'], count, source, tech)
        else:
            start = param['start']
        
            if param['end'] == 'recent':
                end = datetime(2014,12,31)
            else:
                end = param['end']
#            start = datetime(2014,1,1)
#            end = datetime(2014,12,31)

            if param['bundle'] == 'complete':
                make_url(param['url_template'], param['filetype'], source, tech, start, end)
            else:
                break_dates = rrule(rules[param['bundle']], dtstart = start, until = end)
                for date in break_dates:
                    p_start = date.replace(day = 1)
                    if param['bundle'] == 'YEARLY':
                        p_end = p_start + relativedelta(years = 1)
                    if param['bundle'] == 'MONTHLY':
                        p_end = p_start + relativedelta(months = 1)

                    make_url(param['url_template'], param['filetype'], source, tech, p_start, p_end)         


# # 3. Process the data

# We want to merge the downloadet files into one big CSV file. Since every TSO provides the data in a different format, this requires custom read functionality for every source.

# ## 3.1 Read function

# In[ ]:

def readData(filePath, source, tech):
    """Read data from a CSV file taking into account source peculiarities"""

    actual_col = source+'_'+tech+'_actual'
    forecast_col = source+'_'+tech+'_forecast'   
    
    if os.path.getsize(filePath) < 128:
        logger.info("file is smaller than 128 Byte, which means it's probably empty")
        return pd.DataFrame() # return an empty DataFrame 
    
    elif source == 'TransnetBW':
        data = pd.read_csv(
            filePath,
            sep = ";",
            header = 0,
            index_col = "dt_index",
            names = ['input_date', 'input_time', forecast_col, actual_col],
            parse_dates = {'dt_index' : ['input_date', 'input_time',]},
            date_parser = None,         
            dayfirst = True,
#            decimal = ',', #shouln't be relevant
#            thousands = '.',
            converters = None,
            usecols = [0,1,4,5],
        )
        
        data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')
        
    elif source == 'Amprion':
        data = pd.read_csv(
            filePath,
            sep = ";",
            header = 0,
            index_col = 'dt_index',
            names = ['input_date', 'input_time', forecast_col, actual_col],
            parse_dates = {'dt_index' : ['input_date', 'input_time']},
            date_parser = None,
            dayfirst = True,
#            decimal = ',', #shouln't be relevant
#            thousands = '.',
            converters = {'input_time' : lambda x: x[:5]},
            usecols = [0,1,2,3],        
        )
        
        dst_arr = np.ones(len(data.index), dtype=bool)        
        data.index = data.index.tz_localize('Europe/Berlin', ambiguous=dst_arr)

    if source == '50hertz':
        data = pd.read_csv(
            filePath,
            sep = ";",
            header = 3,
            index_col = 'dt_index',
            names = ['input_date', 'input_time', actual_col],
            parse_dates = {'dt_index' : ['input_date', 'input_time']},
            date_parser = None,
            dayfirst = True,
            decimal = ',',
            thousands = '.',
            converters = {'input_time' : lambda x: x[:5]},
            usecols = [0,1,3],
#            engine = 'c', # only the C-engine allows to set the dtype parameter
#            dtype = 'str', # python uses float as the datatype, this sometimes causes changes in the last decimal place
        )
        
#        if not 2006 > pd.to_datetime(data.index.values[0]).year > 2015: # True outside of the interval. Until 2006 as well as  in 2015, only the B-hour in October is present, the A-hour is missing in the data 
        if pd.to_datetime(data.index.values[0]).year not in range(2007,2015):
            dst_arr = np.zeros(len(data.index), dtype=bool)
            data.index = data.index.tz_localize('Europe/Berlin', ambiguous=dst_arr)
        else:
            data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')
            
    if source == 'TenneT':
        data = pd.read_csv(
            filePath,
            sep = ";",
            encoding = 'latin_1',
            header = 3,
            index_col = None,
            names=['input_date', 'position' ,forecast_col, actual_col, source+'_'+tech+'_offshore_share'],
            parse_dates = False,
            date_parser = None,
            dayfirst = True,
#            decimal = ',',
#            thousands = '.',
            converters = None,          
            usecols = list(range(5)),
        )
        
        data['input_date'].fillna(method='ffill', limit = 100, inplace=True)
        
        for i in range(len(data.index)):
            if data['position'][i] > 96:
                logger.info('%s th quarter-hour at %s, position %s',data['position'][i], data.ix[i,'input_date'], (i))  

                if (data['position'] == 101).any():
                    if data['position'][i] == 101: 
                        #data = data.drop(i-101+13)
                        data = data[~((data['input_date'] == data['input_date'][i]) & (data['position'] == 13))]
                        slicer = data[(data['input_date'] == data['input_date'][i]) & (data['position'] >= 13)].index
                        data.loc[slicer, 'position'] = data['position'] - 5
                    
                elif data['position'][i] == 100:
                    slicer = data[(data['input_date'] == data['input_date'][i]) & (data['position'] >= 13)].index
                    data.loc[slicer, 'position'] = data['position'] - 4     
                
            elif data['position'][i] == 92 and ((i == len(data.index)-1) or (data['position'][i+1] == 1)):
                slicer = data[(data['input_date'] == data['input_date'][i]) & (data['position'] >= 9)].index
                data.loc[slicer, 'position'] = data['position'] + 4
        
        data['hour'] = (np.trunc((data['position']-1)/4)).astype(int).astype(str)
        data['minute'] = (((data['position']-1)%4)*15).astype(int).astype(str)
        data['dt_index'] = data['input_date']+' '+data['hour']+':'+data['minute']
        data['dt_index'] = pd.to_datetime(data['dt_index'], dayfirst = True)
        data.set_index('dt_index',inplace=True)

        for crucial_date in pd.to_datetime(['2006-03-26','2008-03-30','2009-03-29']).date:
            if data.index[0].year == crucial_date.year:
                data = data[~((data.index.date == crucial_date) & (data.index.hour == 2))]
        
        data.index = data.index.tz_localize('Europe/Berlin', ambiguous='infer')

#        data = data.drop(['position', 'input_date', 'hour', 'minute'], axis=1)
        if tech == 'wind':
            data = data[[actual_col, forecast_col, source+'_'+tech+'_offshore_share']]
        if tech == 'pv':
            data = data[[actual_col, forecast_col]]

    return data


# ## 3.2 reading files one by one

# For each source/TSO and technology specified in the conf dict, this section finds all the downloaded files in the downloads folder and then calls the readData function with the relevant parameters on each file.
# The datasets returned by the read function are then merged into one large dataset.

# In[ ]:

resultDataSet = pd.DataFrame()
for source, tech in conf.items():
    for tech, param in tech.items():
        for filename in os.listdir(downloadpath):
            if source in filename:
                if tech in filename:
                    logger.info('reading %s', filename)
                    dataToAdd = readData(downloadpath + filename, source, tech)
                    resultDataSet = resultDataSet.combine_first(dataToAdd)


# ## 3.3 Display the Dataset

# This section can be executed to display a preview of the merged dataset.

# In[ ]:

resultDataSet


# # 4. Save csv file to disk

# Finally, we write the data to csv format and save it in the directory specified in the settings section.

# In[ ]:

resultDataSet.to_csv(outputpath+outputfile, sep=';')


# # Notes

# * boolean slicing mit not
# * Am Anfang eine Box mit Userinputs wie path, zeitraum (könnte man auch mit GUI machen)
# * Einlesen unabhängig von Ordnerstruktur
# * Ordnerstruktur visualisieren
# 
# * TransnetBW Fehler (falsche Stundenangaben) 29.03.2015 03:30 - 05:00 wurde behoben in TransnetBW_wind_8(mwindeinsp_ist_prognose_2015_03.csv)_KORRIGIERT.csv
# 
# * datetime.timedelta instead of dateutil.relativedelta
# 
# * bei end date 31.12 statt 01.01.
# * TransnetBW  und normalen Download integrieren
# 
# * p_end eleganter festlegen
