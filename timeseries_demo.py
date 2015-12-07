
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

downloadpath = 'downloads1/'
outputpath = 'output1/'
outputfile = 'output_1.csv'
if not os.path.exists(downloadpath): os.makedirs(downloadpath)
if not os.path.exists(outputpath): os.makedirs(outputpath)


# ## 1.3 Parameters for download sources

# This is a python dictionary containing input parameters needed to generate the URLs belonging to the data sources

# In[ ]:

conf = """    
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

for source, tech in conf.items():
    for tech, param in tech.items():
        if source == 'TransnetBW': #TransnetBW just counts the months backwards, this requires a different approach
            for count in range(12,24): #iterates over range from 12 to 23
                make_url_TransnetBW(param['url_template'], param['filetype'], count, source, tech)
        elif source == 'Amprion':
            start = datetime(2014,1,1)
            end = datetime(2014,12,31)
            make_url(param['url_template'], param['filetype'], source, tech, start, end)     


# # 3. Process the data
# 
# We want to merge the downloadet files into one big CSV file. Since every TSO provides the data in a different format, this requires custom read functionality for every source.

# ## 3.1 Read function

# In[ ]:

def readData(filePath, source, tech):
    """Read data from a CSV file taking into account source peculiarities"""
    
    if os.path.getsize(filePath) < 128:
        print("file is smaller than 128 Byte, which means it's probably empty")
        data = pd.DataFrame() # an empty DataFrame
        return data

    elif source == 'TransnetBW':
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
            converters = None,
            usecols = [0,1,4,5],
        )
        
        data.index = data.index.tz_localize('Europe/Berlin', ambiguous = 'infer')
        
    elif source == 'Amprion':
        data = pd.read_csv(
            filePath,
            sep = ";",
            header = 0,
            index_col = 'dt_index',
            names = ['input_date', 'input_time', source+'_'+tech+'_forecast', source+'_'+tech+'_actual'],
            parse_dates = {'dt_index' : ['input_date', 'input_time']},
            date_parser = None,
            dayfirst = True,
            decimal = ',',
            converters = {'input_time' : lambda x: x[:6]},
            usecols = [0,1,2,3],        
        )
        
        dst_col = np.ones(len(data.index), dtype=bool)        
        data.index = data.index.tz_localize('Europe/Berlin', ambiguous=dst_col)
        
    actualCol = source+'_'+tech+'_actual'    
    forecastCol = source+'_'+tech+'_forecast'
    data = data[[actualCol, forecastCol]]
    
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
                    logger.info('reading')
                    logger.info(filename)
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

