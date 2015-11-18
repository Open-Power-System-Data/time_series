
# coding: utf-8

# # Timeseries download - work in progress
# 
# So far, this script only downloads solar and wind timeserie from several european TSOs

# #Libraries
# Loading some python libraries

# In[73]:

import yaml
import requests
import logging
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.WARNING)
#logger = logging.getLogger()
#logger.setLevel('INFO')
import os
from dateutil.rrule import *
from dateutil.relativedelta import *
from datetime import datetime


# # Folder path
# This section creates a folder inside the notebook's directory on the users computer where data are downloaded to.

# In[36]:

path = 'downloads/'
if not os.path.exists(path): os.makedirs(path)


# # Parameters for download sources
# This is a python dictionary containing input parameters needed to generate the URLs belonging to the data sources

# In[74]:

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
            filetype: xlsx        
        wind2: 
            url_template: http://publications.elia.be/Publications/Publications/WindForecasting.v2.svc/ExportForecastData?beginDate={u_start:%Y-%m-%d}T23%3A00%3A00.000Z&endDate={u_end:%Y-%m-%d}T22%3A00%3A00.000Z&isOffshore=&isEliaConnected=
            bundle: MONTHLY
            start: 2012-03-01
            end: 2012-04-01
            filetype: xlsx        
        wind3: 
            url_template: http://publications.elia.be/Publications/Publications/WindForecasting.v2.svc/ExportForecastData?beginDate={u_start:%Y-%m-%d}T22%3A00%3A00.000Z&endDate={u_end:%Y-%m-%d}T22%3A00%3A00.000Z&isOffshore=&isEliaConnected=
            bundle: MONTHLY
            start: 2012-04-01
            end: recent
            filetype: xlsx                
        pv: 
            url_template: http://publications.elia.be/Publications/Publications/SolarForecasting.v3.svc/ExportSolarForecastGraph?dateFrom={u_start:%Y-%m-%d}T23%3A00%3A00.000Z&dateTo={u_end:%Y-%m-%d}T23%3A00%3A00.000Z&sourceId=1
            bundle: MONTHLY
            start: 2012-11-14
            end: recent
            filetype: xlsx     
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
    CEPS: 
        wind_pv: 
            url_template: http://www.ceps.cz/_layouts/15/Ceps/_Pages/GraphData.aspx?mode=xlsx&from={u_start:%m.%d.%Y}%2012:00:00%20AM&to={u_end:%m/%d/%Y}%2011:59:59%20PM&hasinterval=False&sol=26&lang=ENG&agr=QH&fnc=SUM&ver=RT&para1=all&
            bundle: complete
            start: 2012-01-01
            end: recent
            filetype: xlsx      
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
            url_template: http://www.tennettso.de/site/de/phpbridge?commandpath=Tatsaechliche_und_prognostizierte_Windenergieeinspeisung%2FmonthDataSheetCsv.php&querystring=monat%3D{u_start:%Y-%m&contenttype=text%2Fx-csv
            bundle: MONTHLY
            start: 2005-07-13
            end: recent
            filetype: csv
        
        pv: 
            url_template: http://www.tennettso.de/site/de/phpbridge?commandpath=Tatsaechliche_und_prognostizierte_Solarenergieeinspeisung%2FmonthDataSheetCsv.php&sub=total&querystring=monat%3D{u_start:%Y-%m&contenttype=text%2Fx-csv
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
            filetype: zip xls 
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


# #Some Functions
# In this section we define some functions that generate URLS from parameters
# 
# This is the code that is used to make URLS for all files (takes Y min at 50 kBit)

# In[75]:

def make_url(url_template, source, tech, start, end):
    filename = source+'_'+tech+'_'+start.strftime('%Y-%m-%d')+'_'+end.strftime('%Y-%m-%d')
    full_url = url_template.format(u_start = start, u_end = end)
    download(full_url, filename)


# This is for testing only: only one file per source is downloaded (takes X min at 50 kBit)

# In[76]:

def make_url(i, url_template, source, tech, start, end):
    if i < 1: #for purpose of faster testing, limit number of files downloaded per technology per TSO to 1
        filename = source+'_'+tech+'_'+start.strftime('%Y-%m-%d')+'_'+end.strftime('%Y-%m-%d')
        full_url = url_template.format(u_start = start, u_end = end)
        download(full_url, filename)
        i+=1
        return i
    else:
        return i


# TransnetBW requires to make URLs in a different fashion:

# In[77]:

def make_url_TransnetBW(i, url_template, count, source, tech):
    if i < 1: #for purpose of faster testing, limit number of files downloaded per technology per TSO to 1
        name = source+'_'+tech+'_'+str(count)
        full_url = url_template.format(month = count)
        download(full_url, name)
        i+=1
        return i
    else:
        return i


# This does the actual download:

# In[87]:

def download(full_url, filename):
    print('attempting download of ', filename)    
    print(full_url)
    file = requests.get(full_url, stream = True)
    with open(path+filename+'.csv', 'wb') as output_file:
        for chunk in file.iter_content(1024):
            output_file.write(chunk)
    logging.warning('done')   


# # Extension for rrule
# 

# In[79]:

rules = {'YEARLY': YEARLY,
         'MONTHLY': MONTHLY,
         'DAILY': DAILY}


# #Iterate over sources
# 
# Here we iterate over the sources and technology (wind/solar) entries specified above and download the data for all periods (depending on whats specified under 'bundle' either years, days or months) 

# In[88]:

for source, tech in conf.items():
    for tech, param in tech.items():
        i = 0
        if source == 'TransnetBW': #TransnetBW just counts the months backwards, this requires a different approach
            for count in range(0,70): #iterates over range from 0 to 69
                i = make_url_TransnetBW(i, param['url_template'], count, source, tech)
            continue #the following steps are skipped for TransnetBW
        start = param['start']
        if type(param['end']) == datetime.date:
            end = param['end']
        else:
            end = datetime(2015,1,1)
        if param['bundle'] == 'complete':
            i = make_url(i, param['url_template'], source, tech, param['start'], param['end'])
        else:
            break_dates = rrule(rules[param['bundle']], dtstart = start, until = end)
            for date in break_dates:
                p_start = date.replace(day = 1)
                #onedaylater = p_start+relativedelta(days=+1)
                #p_end = dates_where_data_breaks.after(onedaylater, inc = False)
                if param['bundle'] == 'YEARLY':
                    p_end = p_start + relativedelta(years = 1)
                if param['bundle'] == 'MONTHLY':
                    p_end = p_start + relativedelta(months = 1)
                if source == 'Elia':
                    p_start = p_start - relativedelta(days = 1) 
                    p_end = p_end - relativedelta(days = 1)
                i = make_url(i, param['url_template'], source, tech, p_start, p_end)         


# In[14]:

temp = rruleset(rrule(rules[param['bundle']], dtstart=start, until=end))


# In[15]:

type(temp)


# In[36]:

a = temp.before(datetime(2010, 8, 31, 9, 0))
print(a)


# In[26]:

dates_where_data_breaks.rrule(rrule(rules[param['bundle']], dtstart=start, until=end))
print(list(dates_where_data_breaks))
print(type(dates_where_data_breaks))
for p_start in dates_where_data_breaks:
    onedaylater = p_start+relativedelta(days=1)    
    print(onedaylater)
    print(type(p_start))


# In[61]:

type(param['start'])


# In[66]:

end = datetime(2015,1,1)
type(end)
type(start)


# In[ ]:



