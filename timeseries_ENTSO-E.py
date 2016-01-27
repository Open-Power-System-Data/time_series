
# coding: utf-8

# In[1]:

import pytz
import yaml
import requests
import logging
logger = logging.getLogger('log')
logger.setLevel('INFO')
import os
from dateutil.relativedelta import *
#from datetime import datetime
import datetime
import numpy as np
import pandas as pd


# In[2]:

downloadpath = 'downloads1/'
archivepath = 'archive1/'
outputpath = 'output1/'
outputfile = 'output1.csv'
if not os.path.exists(downloadpath): os.makedirs(downloadpath)
if not os.path.exists(outputpath): os.makedirs(outputpath)


# In[3]:

conf = """
    ENTSO-E: 
        Data_Portal: 
            url_template: https://www.entsoe.eu/fileadmin/template/other/statistical_database/excel.php
            url_params:
                pid: 136
                opt_period: 0
                opt_Month: ''
                opt_Year: ''
                send: send
                opt_Response: 99
                dataindx: 0
            url_dates:
                opt_Month: '{u_start.month}'
                opt_Year: '{u_start.year}'
            x_Month: '{u_start.month}'
            x_Year: '{u_start.year}'
            frequency: M
            start: 2006-01-01
            end: recent
            filetype: xls           
"""
conf = yaml.load(conf)


# In[4]:

def make_url(url_template, filetype, source, tech, start, end, session, url_params):
    """construct URLs from a template, filling in start- and enddates and call download funtion."""    
    filename = source+'_'+tech+'_'+start.strftime('%Y-%m-%d')+'_'+end.strftime('%Y-%m-%d')
#    full_url = url_template.format(u_start = start, u_end = end)
#    download(full_url, filename, filetype, session)
    conf['ENTSO-E']['Data_Portal']['url_params']['opt_Month'] = (
        conf['ENTSO-E']['Data_Portal']['x_Month'].format(u_start = start, u_end = end)
        )
    conf['ENTSO-E']['Data_Portal']['url_params']['opt_Year'] = (
        conf['ENTSO-E']['Data_Portal']['x_Year'].format(u_start = start, u_end = end)
        )
    resp = session.get(url_template, params=url_params)
#    resp = requests.get(url_template, params=url_params)
    
#    import pdb; pdb.set_trace()
#    time.sleep(10)
    
    original_filename = resp.headers['content-disposition'].split('filename=')[-1].replace('"','').replace(';','')
    logger.info('Attempting download of: %s \n From URL: %s \n original filename: %s', filename, resp.url, original_filename)
    work_file = downloadpath+filename+'.'+filetype
    if os.path.exists(work_file):
        logger.info('Filename already exists. Skip to next.')
    else:
        with open(work_file, 'wb') as output_file:
            for chunk in resp.iter_content(1024):
                output_file.write(chunk)


# In[31]:

for source, tech in conf.items():
    for tech, parameter in tech.items():
        session = requests.session()
        g_start = parameter['start']
        g_start = datetime.date(2015,12,1)
        if parameter['end'] == 'recent':
            g_end = datetime.date(2015,12,31)
        else:
            g_end = param['end']

        break_dates = pd.date_range(start=g_start, end=g_end, freq=parameter['frequency'])
        for date in break_dates:
            p_start = date.replace(day = 1)
            if parameter['frequency'] == 'M':
                p_end = p_start + relativedelta(months = 1, days = -1)
            if parameter['frequency'] == 'Y':
                p_end = p_start + relativedelta(years = 1, days = -1)
                
#            make_url(param['url_template'], param['filetype'], source, tech, p_start, p_end, session, param['url_params'])   
            filename = source+'_'+tech+'_'+p_start.strftime('%Y-%m-%d')+'_'+p_end.strftime('%Y-%m-%d')
            parameter['url_params']['opt_Month'] = parameter['x_Month'].format(u_start = p_start, u_end = p_end)
            parameter['url_params']['opt_Year'] = parameter['x_Year'].format(u_start = p_start, u_end = p_end)
            
            resp = session.get(parameter['url_template'], params=parameter['url_params'])
            
            original_filename = resp.headers['content-disposition'].split('filename=')[-1].replace('"','').replace(';','')
            logger.info('Attempting download of: %s \n From URL: %s \n original filename: %s', filename, resp.url, original_filename)
            work_file = downloadpath+filename+'.'+param['filetype']
            if os.path.exists(work_file):
                logger.info('Filename already exists. Skip to next.')
            else:
                with open(work_file, 'wb') as outputfile:
                    for chunk in resp.iter_content(1024):
                        outputfile.write(chunk)


# In[32]:

parameter['url_params'][parameter['url_dates']]


# In[733]:

parameter['url_dates']


# In[23]:

def readData(filePath, source, tech):
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
#    data = data.drop(['Day', 'hour', 'raw_hour'], axis=1, inplace = True)
    data.rename(columns=lambda x: 'load_'+x, inplace=True)
    return data


# In[709]:

def readData(filePath, source, tech):
    if tech in ['wind_pv_1', 'wind_pv_2']:
        skipper = 3
        cols = [0,1,3,4]
        colnames = ['date', 'hour', 'SE_wind_actual', 'SE_hydro']
    elif tech in ['wind_pv_3', 'wind_pv_4', 'wind_pv_5', 'wind_pv_6']:
        if tech == 'wind_pv_4':
            skipper = 4
        else:
            skipper = 6
        cols = [0,2,3,8]
        colnames = ['datetime', 'SE_wind_actual', 'SE_hydro', 'SE_pv_actual']
        
    data = pd.read_excel(
        io = filePath,
        sheetname = -1,
        header = None,
        skiprows = skipper,
        index_col = None,
        parse_cols = cols
        )

#    data.reset_index(inplace=True)
#    if 'index' in data.columns: data.drop('index', axis = 1, inplace = True)
        
    data.columns = colnames

    if tech in ['wind_pv_1', 'wind_pv_2']:
        data = data[data['date'].notnull()] #applies to 2009
        data['dt_index'] = pd.to_datetime(data['date'].astype(int).astype(str)+' '+data['hour'].astype(int).astype(str).str.replace('00','')+':00', dayfirst = False, infer_datetime_format = True)
        data.drop(['date','hour'], axis=1, inplace = True)
    else:
        data = data[((data['datetime'].notnull()) & (data['datetime'].astype(str) != 'Tot summa GWh'))] #applies to 2011
        data['dt_index'] = pd.to_datetime(data['datetime'], dayfirst = True)#, infer_datetime_format = True)
        data.drop(['datetime'], axis=1, inplace = True)
        
    data.set_index('dt_index', inplace=True)
    data.index = (data.index.tz_localize('UTC') + pd.offsets.Hour(-1)).tz_convert('Europe/Berlin')   
    
    return data
    


# In[24]:

resultDataSet = pd.DataFrame()
for source, tech in conf.items():
    for tech, param in tech.items():
        for filename in os.listdir(downloadpath):
            if source in filename and tech in filename:
                logger.info('reading %s', filename)
                dataToAdd = readData(downloadpath + filename, source, tech)
                resultDataSet = dataToAdd#resultDataSet.combine_first(dataToAdd)


# In[22]:

resultDataSet #['2015-10-24']


# In[ ]:

#tz = pytz.timezone("Europe/Berlin")
#slicer = pd.DatetimeIndex(pytz.timezone('Europe/Berlin')._utc_transition_times[1:]).tz_localize('UTC').tz_convert('Europe/Berlin').tz_localize(None)
#slice_list = list(slicer[slicer.month == 10])
slicer = pytz.timezone('Europe/Berlin')._utc_transition_times[1:]
slicer = [d.replace(hour=2) for d in slicer if d.month == 3]
slicer


# In[34]:

datetime.datetime(2007, 3, 26, 1, 0).ctime()


# In[83]:

data = resultDataSet
slicer = pd.DatetimeIndex(pytz.timezone('Europe/Berlin')._utc_transition_times[1:]).tz_localize('UTC').tz_convert('Europe/Berlin').tz_localize(None)
slicer = slicer[slicer.month == 10]
data.index not in slicer]


# In[ ]:

März: 1 eintra rausschmeißen
Oktober: 30 einträge rausschmeißen


# In[121]:

import pandas as pd
idx = pd.date_range(start='2005-01-01', end='2015-12-31', freq='D')
data = pd.DataFrame(index=idx)
data['capacity_pv_50Hertz']= 'Hier Code für Summe über PV-Kapazität in 50Hertz Regelzone An einem Stichtag einfügen'
data_to_merge_with_other_timeseries = data.resample('15min', fill_method='ffill')


# In[ ]:

data_to_merge_with_other_timeseries


# In[649]:

resultDataSet.to_csv(outputpath+outputfile, sep=';', float_format='%.2f', decimal=',')


# In[ ]:



