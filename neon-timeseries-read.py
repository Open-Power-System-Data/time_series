
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
import pandas as pd


# # 2. Folder path

# This section creates a folder inside the notebook's directory on the users computer where data are read from.

# In[18]:

downloadpath = 'downloads/'
outputpath = 'output/'
if not os.path.exists(outputpath): os.makedirs(outputpath)


# In[3]:

conf = """
    Fingrid: 
        wind: 
            url_template: http://www.fingrid.fi/en/electricity-market/load-and-generation/windpower/Pages/default.aspx/Excel/TimeSeries.xls?beginDate=20150101&endDate=20151231&variables=H4sIAAAAAAAEAO29B2AcSZYlJi9tynt_SvVK1-B0oQiAYBMk2JBAEOzBiM3mkuwdaUcjKasqgcplVmVdZhZAzO2dvPfee--999577733ujudTif33_8_XGZkAWz2zkrayZ4hgKrIHz9-fB8_Ih7_ZFYX2aTMmyP769nyvDp6_CJb5EfPzn7_N1999fzsJ788--L49_99vv3m8V3-_PHZ8jKv25_MynV-dJ6VTf74rv_RY_7xrKoXWfu6rYvlxdGPj378x3ce3-1_8firZdG-yd-1R198d353_viu_fvxm6It86PvFstZuqqu8jq9yJd5nbVFtUy3uPWdx3el0ePnxTI_qcqqTu96f3y7XZRHP_7k5MnDHeo8_Pjx63l19eWyvH62btd1_jRrMzOWyDdA3aeP_bM5-n8AhQmq0kUBAAA1&cultureId=en-US&dataTimePrecision=5
            bundle: MONTHLY
            start: 2014-11-28
            end: recent
            filetype: xls
    Elia: 
        wind1: 
            url_template: http://publications.elia.be/Publications/Publications/WindForecasting.v2.svc/ExportForecastData?beginDate={u_start:%Y-%m-%d}T23%3A00%3A00.000Z&endDate={u_end:%Y-%m-%d}T23%3A00%3A00.000Z&isOffshore=&isEliaConnected=
            bundle: MONTHLY
            start: 2012-01-01  #the data starts from 2012-19-01
            end: 2012-03-01
            filetype: xls        
        wind2: 
            url_template: http://publications.elia.be/Publications/Publications/WindForecasting.v2.svc/ExportForecastData?beginDate={u_start:%Y-%m-%d}T23%3A00%3A00.000Z&endDate={u_end:%Y-%m-%d}T22%3A00%3A00.000Z&isOffshore=&isEliaConnected=
            bundle: MONTHLY
            start: 2012-03-01
            end: 2012-04-01
            filetype: xls        
        wind3: 
            url_template: http://publications.elia.be/Publications/Publications/WindForecasting.v2.svc/ExportForecastData?beginDate={u_start:%Y-%m-%d}T22%3A00%3A00.000Z&endDate={u_end:%Y-%m-%d}T22%3A00%3A00.000Z&isOffshore=&isEliaConnected=
            bundle: MONTHLY
            start: 2012-04-01
            end: recent
            filetype: xls                
        pv: 
            url_template: http://publications.elia.be/Publications/Publications/SolarForecasting.v3.svc/ExportSolarForecastGraph?dateFrom={u_start:%Y-%m-%d}T23%3A00%3A00.000Z&dateTo={u_end:%Y-%m-%d}T23%3A00%3A00.000Z&sourceId=1
            bundle: MONTHLY
            start: 2012-11-14
            end: recent
            filetype: xls     
    Amprion: 
        wind: 
            url_template: http://preview.amprion.de/applications/applicationfiles/winddaten.php?mode=download&format=csv&start={u_start:%Y.%m.%d}&end={u_end:%Y.%m.%d}
            bundle: complete
            start: 2006-01-03
            end: recent
            filetype: csv 
        pv: 
            url_template: http://amprion.de/applications/applicationfiles/PV_einspeisung.php?mode=download&format=csv&start={u_start:%Y.%m.%d}&end={u_end:%Y.%m.%d}
            bundle: complete
            start: 2010-01-07
            end: recent
            filetype: csv        
#    CEPS: 
#        wind_pv: 
#            url_template: http://www.ceps.cz/_layouts/15/Ceps/_Pages/GraphData.aspx?mode=xlsx&from={u_start:%m.%d.%Y}%2012:00:00%20AM&to={u_end:%m/%d/%Y}%2011:59:59%20PM&hasinterval=False&sol=26&lang=ENG&agr=QH&fnc=SUM&ver=RT&para1=all&
#            bundle: complete
#            start: 2012-01-01
#            end: recent
#            filetype: xlsx      
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
            start: 2005-07-13
            end: recent
            filetype: csv
        
        pv: 
            url_template: http://www.tennettso.de/site/de/phpbridge?commandpath=Tatsaechliche_und_prognostizierte_Solarenergieeinspeisung%2FmonthDataSheetCsv.php&sub=total&querystring=monat%3D{u_start:%Y-%m}&contenttype=text%2Fx-csv
            bundle: MONTHLY
            start: 2010-01-01
            end: recent
            filetype: csv   
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
    RTE: 
        wind_pv: 
            url_template: http://clients.rte-france.com/servlets/RealProdServlet?annee={u_start:%Y}
            bundle: YEARLY
            start: 2014-01-01
            end: recent 
            filetype: zip #xls 
    Svenska_Kraftnaet: 
        wind_pv_1: 
            url_template: http://www.svk.se/siteassets/aktorsportalen/statistik/sverigestatistik/n_fot{u_start:%Y}.xls
            bundle: YEARLY
            start: 2002-01-01
            end: 2009-01-01
            filetype: xls        
        wind_pv_2: 
            url_template: http://www.svk.se/siteassets/aktorsportalen/statistik/sverigestatistik/n_fot201001-06.xls
            bundle: YEARLY
            start: 2010-01-01
            end: 2010-01-01
            filetype: xls        
        wind_pv_3: 
            url_template: http://www.svk.se/siteassets/aktorsportalen/statistik/sverigestatistik/n_fot2010-07-12.xls
            bundle: YEARLY
            start: 2010-01-01
            end: 2010-01-01
            filetype: xls       
        wind_pv_4: 
            url_template: http://www.svk.se/siteassets/aktorsportalen/statistik/sverigestatistik/n-fot2011-01-12.xls
            bundle: YEARLY
            start: 2011-01-01
            end: 2011-01-01
            filetype: xls        
        wind_pv_5: 
            url_template: http://www.svk.se/siteassets/aktorsportalen/statistik/sverigestatistik/n_fot{u_start:%Y}-01-12.xls
            bundle: YEARLY
            start: 2012-01-01
            end: 2014-01-01
            filetype: xls    
    OeMag: 
        wind_1: 
            url_template: http://www.oem-ag.at/fileadmin/user_upload/Dokumente/statistik/winderzeugung/winderzeugung_{u_start:%Y}.xls
            bundle: YEARLY
            start: 2003-01-01
            end: 2011-01-01
            filetype: xls        
        wind_2: 
            url_template: http://www.oem-ag.at/fileadmin/user_upload/Dokumente/statistik/winderzeugung/winderzeugung_{u_start:%Y}.xlsx
            bundle: YEARLY
            start: 2012-01-01
            end: recent
            filetype: xls
"""
conf = yaml.load(conf)


# In[4]:

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


# In[16]:

def readData(filePath, source, tech):
    if os.path.getsize(filePath) < 128:
        print("file is smaller than 128 Byte, which means it's probably empty")
        data = pd.DataFrame() # return empty DataFrame
        return data
    if source = 'TransnetBW':
        data = pd.read_csv(
            filePath,
            decimal=',',
            sep=";",
            parse_dates = {'Timestamp' : ['Datum von', 'Uhrzeit von']},
            index_col = "Timestamp",
            dayfirst=True,
        )

#        data = data.drop('Unnamed: 6', 1)
        data = data.drop('Datum bis', axis=1)
        data = data.drop('Uhrzeit bis', axis=1)
#        data.fillna(0,inplace=True)
    
        forecastCol = source+'_'+tech+'_forecast'
        actualCol = source+'_'+tech+'_forecast'

        data.rename(
            columns={'Prognose (MW)': forecastCol, 'Ist-Wert (MW)': actualCol},
            inplace=True
        )
    
        if 'Datum' in data.columns: 
            del data['Datum']
        if 'Uhrzeit' in data.columns: 
            del data['Uhrzeit']
    elif source = 'TenneT':
        data = pd.read_csv(
            filePath
        
    
    return data


# In[24]:

rng = pd.date_range(start='01/01/2003 00:00', end='18/11/2015 23:45', freq='15 Min')
resultDataSet = pd.DataFrame(index=rng)
resultDataSet.index.names = ['Timestamp']
for source, tech in conf.items():
    for tech, param in tech.items():
        for filename in os.listdir(downloadpath):
            if source in filename:
                if tech in filename:
                    print('reading' filename)
                    try:
                        dataToAdd = readData(downloadpath + filename, source, tech)
                        resultDataSet = resultDataSet.combine_first(dataToAdd)
                    except ValueError:
                        atLeastOneError = 'yes'
                        print('error')
                        
resultDataSet


# In[25]:

resultDataSet.to_csv(outputpath+"Output1.csv", sep=';')


# ### to do
# 
# periodindex ?
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

# In[ ]:



