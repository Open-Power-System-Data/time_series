
# coding: utf-8

# # Download
# 
# Part of the project [Open Power System Data](http://open-power-system-data.org/).
# 
# Find the latest version of this notebook an [GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/download.ipynb)
# 
# Go back to the main notebook ([GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/main.ipynb) / [local copy](main.ipynb))

# First, a data directory is created on your local computer. Then, download parameters for each data source are defined, including the URL. These parameters are then turned into a YAML-string. Finally, the download is executed one by one. If all data need to be downloaded, this usually takes several hours.

# # Table of Contents
# * [1. Preparations](#1.-Preparations)
# 	* [1.1 Libraries](#1.1-Libraries)
# 	* [1.2 Set up a log.](#1.2-Set-up-a-log.)
# 	* [1.3 Create a download directory](#1.3-Create-a-download-directory)
# * [2. Parameters for individual data sources](#2.-Parameters-for-individual-data-sources)
# 	* [2.1 ENTSO-E](#2.1-ENTSO-E)
# 	* [2.2 '50Hertz](#2.2-'50Hertz)
# 	* [2.3 Amprion](#2.3-Amprion)
# 	* [2.4 TransnetBW](#2.4-TransnetBW)
# 	* [2.5 TenneT](#2.5-TenneT)
# 	* [2.6 OPSD](#2.6-OPSD)
# * [3. Download files one by one](#3.-Download-files-one-by-one)
# 

# # 1. Preparations

# ## 1.1 Libraries

# Loading some python libraries.

# In[ ]:

from datetime import datetime, date
import yaml
import requests
import os
import pandas as pd
import logging
import getpass


# ## 1.2 Set up a log.

# In[ ]:

logger = logging.getLogger('log')
logger.setLevel('INFO')


# ## 1.3 Create a download directory

# This section creates a folder "downloadpath" inside the notebook's directory on the user's local computer for the downloaded data.

# In[ ]:

downloadpath = 'original_data'
os.makedirs(downloadpath, exist_ok=True)


# # 2. Parameters for individual data sources

# This section contains a python dictionary for each download source with input parameters needed to generate the URLs for the data.

# ## 2.1 ENTSO-E

# In[ ]:

entso = """
ENTSO-E: 
    load: 
        url_template: https://www.entsoe.eu/fileadmin/template/other/statistical_database/excel.php
        url_params_template:
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


# ## 2.2 '50Hertz

# In[ ]:

hertz = """
50Hertz: 
    wind_generation: 
        url_template: http://ws.50hertz.com/web01/api/WindPowerActual/DownloadFile
        url_params_template:
            callback: '?'
            fileName: '{u_start:%Y}.csv'
        frequency: A
        start: 2005-01-01
        end: recent
        filetype: csv
    solar_generation: 
        url_template: http://ws.50hertz.com/web01/api/PhotovoltaicActual/DownloadFile
        url_params_template:
            callback: '?'
            fileName: '{u_start:%Y}.csv'
        frequency: A
        start: 2012-01-01
        end: recent
        filetype: csv
    wind_forecast: 
        url_template: http://ws.50hertz.com/web01/api/WindPowerForecast/DownloadFile
        url_params_template:
            callback: '?'
            fileName: '{u_start:%Y}.csv'
        frequency: A
        start: 2005-01-01
        end: recent
        filetype: csv
    solar_forecast: 
        url_template: http://ws.50hertz.com/web01/api/PhotovoltaicForecast/DownloadFile
        url_params_template:
            callback: '?'
            fileName: '{u_start:%Y}.csv'
        frequency: A
        start: 2012-01-01
        end: recent
        filetype: csv
"""


# ## 2.3 Amprion

# In[ ]:

amprion = """
Amprion:
    wind: 
        url_template: http://amprion.de/applications/applicationfiles/winddaten2.php
        url_params_template:
            mode: download
            format: csv
            start: '{u_start.day}.{u_start.month}.{u_start.year}'
            end: '{u_end.day}.{u_end.month}.{u_end.year}' # dates must not be zero-padded
        frequency: complete
        start: 2008-01-04
        end: recent
        filetype: csv
    solar: 
        url_template: http://amprion.de/applications/applicationfiles/PV_einspeisung.php
        url_params_template:
            mode: download
            format: csv
            start: '{u_start.day}.{u_start.month}.{u_start.year}'
            end: '{u_end.day}.{u_end.month}.{u_end.year}' # dates must not be zero-padded        
        frequency: complete
        start: 2010-01-07
        end: recent
        filetype: csv
"""


# ## 2.4 TransnetBW

# In[ ]:

transnetbw = """
TransnetBW: 
    wind: 
        url_template: https://www.transnetbw.de/de/kennzahlen/erneuerbare-energien/windenergie
        url_params_template:
            app: wind
            activeTab: csv
            view: '1'
            download: 'true'
            selectMonatDownload: '{u_transnetbw}'
        frequency: M
        start: 2010-01-01
        end: recent
        filetype: csv
    solar: 
        url_template: https://www.transnetbw.de/de/kennzahlen/erneuerbare-energien/fotovoltaik
        url_params_template:
            app: wind
            activeTab: csv
            view: '1'
            download: 'true'
            selectMonatDownload: '{u_transnetbw}'
        frequency: M
        start: 2011-01-01
        end: recent
        filetype: csv
"""


# ## 2.5 TenneT

# In[ ]:

tennet = """
TenneT: 
    wind: 
        url_template: http://www.tennettso.de/site/de/phpbridge
        url_params_template:
            commandpath: Tatsaechliche_und_prognostizierte_Windenergieeinspeisung/monthDataSheetCsv.php
            contenttype: text/x-csv
            querystring: monat={u_start:%Y-%m}
        frequency: M
        start: 2006-01-01
        end: recent
        filetype: csv        
    solar: 
        url_template: http://www.tennettso.de/site/de/phpbridge
        url_params_template:
            commandpath: Tatsaechliche_und_prognostizierte_Solarenergieeinspeisung/monthDataSheetCsv.php
            sub: total
            contenttype: text/x-csv
            querystring: monat={u_start:%Y-%m}
        frequency: M
        start: 2010-01-01
        end: recent
        filetype: csv  
"""


# ## 2.6 OPSD

# In[ ]:

opsd = """
OPSD:
    capacities:
        url_template: http://data.open-power-system-data.org/datapackage_renewables/2016-03-09/renewable_capacity_germany_timeseries.csv
        url_params_template: 
        frequency: complete
        start: 2005-01-01
        end: recent
        filetype: csv
"""


# # 3. Download files one by one

# Load the parameters for the data sources we wish to include into a [YAML](https://en.wikipedia.org/wiki/YAML)-string.
# 
# To select whith data sources to download, adjust the list ``datasets_to_download`` accordingly, e.g.:
# 
#     datasets_to_download = hertz + tennet

# In[ ]:

datasets_to_download = hertz + amprion + tennet + transnetbw + entso + opsd

conf = yaml.load(datasets_to_download)


# In the following we iterate over the sources and resources (load/wind/solar, forecast/generation/capacity) specified above and download the data for a the period given in the parameters. Each file is saved under it's original filename. Note that the original file names are often not self-explanatory (called "data" or "January"). The files content is revealed by its place in the directory structure.

# In[ ]:

def download(session, source, resource, p, start, end):
    """construct URLs from template and parameters, download, and save."""
    
    logger.info('Proceed to download: {} {} {:%Y-%m-%d}_{:%Y-%m-%d}'.format(
                source, resource, start, end))
    
    # Get number of months between now and start (required for TransnetBW).
    count = datetime.now().month - start.month + (datetime.now().year-start.year)*12

    # Create the parameters dict containing timespan info to be pasted with url
    if p['url_params_template']:
        url_params = {}
        for key, value in p['url_params_template'].items():
            url_params[key] = value.format(u_start=start,
                                           u_end=end,
                                           u_transnetbw=count)

    # Each file will be saved in a folder of its own, this allows us to preserve
    # the original filename when saving to disk.  
    container = os.path.join(downloadpath, source, resource,
                             start.strftime('%Y-%m-%d') + '_' +
                             end.strftime('%Y-%m-%d'))
    os.makedirs(container, exist_ok=True)
    
    # Attempt the download if there is no file yet.  
    count_files =  len(os.listdir(container))   
    if count_files == 0:
        if source == 'OPSD':
            if 'MORPH_OPSD_BETA_PW' in os.environ:
                password = os.environ['MORPH_OPSD_BETA_PW']
            else:
                password = getpass.getpass('Please enter the beta-user password:')
            password = getpass.getpass('Please enter the beta-user password:')
            resp = session.get(p['url_template'], auth=('beta', password))
            original_filename = p['url_template'].split('/')[-1]
        else:
            resp = session.get(p['url_template'], params=url_params)                
            original_filename = resp.headers['content-disposition'].split(
                'filename=')[-1].replace('"','').replace(';','')              
        logger.info('Downloading from URL: %s Original filename: %s',
                    resp.url, original_filename)
        filepath = os.path.join(container, original_filename)
        with open(filepath, 'wb') as output_file:
            for chunk in resp.iter_content(1024):
                output_file.write(chunk)                
    elif count_files == 1:
        logger.info('There is already a file: %s', os.listdir(container)[0])
    else:
        logger.info('There must not be more than one file in: %s. Please check ',
                    container)
        
    return


for source, resources in conf.items():
    for resource, p in resources.items():
        session = requests.session()
#        p['start'] = date(2015,1,1) # uncomment this to set a different start
        if p['end'] == 'recent':
            p['end'] = date(2015,12,31)

        if p['frequency'] == 'complete':
            download(session, source, resource, p, start=p['start'], end=p['end'])            
        else:
            # The files on the servers usually contain the data for subperiods
            # of some regular length (i.e. months or yearsavailable 
            # Create lists of start- and enddates of periods represented in
            # individual files to be downloaded.  
            starts = pd.date_range(start=p['start'], end=p['end'],
                                   freq=p['frequency']+'S')
            ends = pd.date_range(start=p['start'], end=p['end'],
                                 freq=p['frequency'])
            for s, e in zip(starts, ends):
                download(session, source, resource, p, start=s, end=e)


# As a next step, the downloaded files should be read and combined with the read-script ([local copy](read.ipynb) / [GitHub](https://github.com/Open-Power-System-Data/datapackage_timeseries/blob/master/read.ipynb))
