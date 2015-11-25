
# coding: utf-8

# # Table of Contents
# * [1. Importing Libraries](#1.-Importing-Libraries)
# * [2. Folder path](#2.-Folder-path)
# 

# # 1. Importing Libraries

# os is used to for the path
# 
# pandas is used to read CSVs

# In[1]:

import yaml
import os
import numpy as np
import pandas as pd
import pytz


# # 2. Folder path

# This section defines a folder inside the notebook's directory on the users computer where downloaded data are read from as well as another folder and file for the output, which contains the merged data from the TSOs

# In[2]:

downloadpath = 'downloads/'
outputpath = 'output/'
outputfile = 'output_1.csv'
if not os.path.exists(outputpath): os.makedirs(outputpath)


# # Input Parameters
# 
# Here we define which Sources/TSOs and technologies the skript should look for in the downloads folder.
# At the moment, We use just the parameters dict from the downloads script, although most of the information contained is superfluous here.
# Later we want to use this section to pass information on the peculiarities of the input CSV data tables like:
# * delimiter character used
# * how many lines to skip at the beginning/end of each file
# * which column contains which data
# * encoding
# * DST handling

# In[4]:

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


# In[14]:

conf = """
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


# In[15]:

def readData(filePath, source, tech):
    """Read data from a CSV file taking into account source peculiarities"""
    
    if os.path.getsize(filePath) < 128:
        print("file is smaller than 128 Byte, which means it's probably empty")
        data = pd.DataFrame() # an empty DataFrame
        return data
    
    if source == 'TransnetBW':
        data = pd.read_csv(
            filePath,
            decimal=',',
            sep=";",
            parse_dates = {'Timestamp' : ['Datum von', 'Uhrzeit von']},
            index_col = "Timestamp",
            dayfirst=True,
        )
        data['test'] = data.index #.tz_localize('UTC', ambiguous = 'NaT').tz_convert('Europe/Berlin')
        data.index = data.index.tz_localize('Europe/Berlin', ambiguous = 'infer')
        
        actualCol = source+'_'+tech+'_actual'    
        forecastCol = source+'_'+tech+'_forecast'

        data.rename(
            columns={'Prognose (MW)': forecastCol, 'Ist-Wert (MW)': actualCol},
            inplace=True
        )

    elif source == 'TenneT':
        data = pd.read_csv(
            filePath,
            sep=";",
            encoding = 'latin_1',
            usecols = list(range(5)),
            #skiprows = 4,
            header=3,
            #index_col = False,
            names=['Datum','Position',source+'_'+tech+'_forecast',source+'_'+tech+'_actual',source+'_'+tech+'_offshore_share']
        )
        data['Datum'].fillna(method='ffill', limit = 99, inplace=True)           
        data['hour'] = (np.trunc((data['Position']-1) /4)).astype(int).astype(str)
        data['minute'] = (((data['Position']-1) % 4)*15).astype(int).astype(str)
        data['time'] = data['Datum']+' '+data['hour']+':'+data['minute']
        data['Timestamp'] = pd.to_datetime(data['time'])
        data.set_index('Timestamp',inplace=True)
        data.index = data.index.tz_localize('Europe/Berlin')

        actualCol = source+'_'+tech+'_actual'    
        forecastCol = source+'_'+tech+'_forecast'
            
    data = data[[actualCol, forecastCol, 'test']]
    return data


# # Iterate over sources
# For each source/TSO and technology specified in the conf dict, this section finds all the downloaded files in the downloads folder and then calls the read function with the relevant parameters on each file.

# In[16]:

resultDataSet = pd.DataFrame()
for source, tech in conf.items():
    for tech, param in tech.items():
        for filename in os.listdir(downloadpath):
            if source in filename:
                if tech in filename:
                    print('reading', filename)
                    dataToAdd = readData(downloadpath + filename, source, tech)
                    resultDataSet = resultDataSet.combine_first(dataToAdd)


# In[17]:

#resultDataSet['2015-01-01':'2015-01-01']
resultDataSet['2015-03-29 00:00:00+01:00':'2015-10-25 06:00:00+01:00']
#resultDataSet


# In[53]:

resultDataSet.to_csv(outputpath+outputfile, sep=';')


# # Notizen
# 
# PeriodIndex statt DatetimeIndex verwenden?
# 
# Iso Format:
# 
# Proposal: several time variables
# • ISO 8601 UTC
# YYYY-MM-DDThh:mm:ssZ
# • ISO 8601 local time (difference to UTC)
# YYYY-MM-DDThh:mm:ss+hh:mm
# • a set of time variables: year, 
# month_year, day_year, hour_day, 
# hour_year, weekday, peak / off-peak
# 
# Was in welcher Spalte steht muss dem Skript mitgeteilt werden, die Spaltennamen in den Dateien helfen nicht. Aber evtl könnte man diese auslesen und den durch das Skript zugewiesenen neuen Spaltenamen gegenüberstellen, um auf Fehler zu überprüfen
# 
# read_csv:
# 
# * usecols
# 
# csv sniffer
# 
# datetime.timedelta instead of dateutil.relativedelta
# 
# Kontrollspalte in der ich selber die Zeit berechne
# 
# TransnetBW Fehler  29.03.2015 03:30 - 05:00 
# 

# In[77]:

rng = pd.date_range(start='01/01/2015 00:00', end='18/11/2015 23:45', freq='60 Min', tz = 'UTC')
#rng.tz_localize('UTC').tz_convert('Europe/Berlin') #, ambiguous = 'infer')
#testDataSet = pd.DataFrame(index=rng)
testDataSet = pd.DataFrame(rng)
testDataSet.index = rng
#testDataSet.index.names = ['timestamp']
testDataSet[0].tz_convert('Europe/Berlin') #, ambiguous = 'NaT') #.tz_convert('Europe/Berlin')
#testDataSet['Berlin'] = testDataSet.index.tz_convert('UTC')
testDataSet['new'] = rng.tz_convert('Europe/Berlin')
#testDataSet['2015-03-29':'2015-10-25 04:00']
#testDataSet
rng


# In[ ]:



