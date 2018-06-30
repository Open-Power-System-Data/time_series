"""
Open Power System Data

Timeseries Datapackage

download.py : download time series files

"""

import argparse
from datetime import datetime, date, time, timedelta
import pytz
import logging
import os
import zipfile
import pandas as pd
import requests
import yaml
from ftplib import FTP
import math
import sys
import time

logger = logging.getLogger(__name__)
logger.setLevel('INFO')


def download(sources, data_path, archive_version=None,
             start_from_user=None, end_from_user=None,
             testmode=False):
    """
    Load YAML file with sources from disk, and download all files for each
    source into the given data_path.

    Parameters
    ----------
    sources : dict
        Dict of download parameters specific to each source.
    data_path : str
        Base download directory in which to save all downloaded files.
    archive_version: str, default None
        OPSD Data Package Version to download original data from.
    start_from_user : datetime.date, default None
        Start of period for which to download the data.
    end_from_user : datetime.date, default None
        End of period for which to download the data.
    testmode: only download 1 file per source to check if the URLs still still work

    Returns
    ----------
    None

    """

    for name, date in {'end_from_user': end_from_user,
                       'start_from_user': start_from_user}.items():
        if date and date > datetime.now().date():
            logger.info('%s given was %s, must be smaller than %s, '
                        'we have no data for the future!',
                        name, date, datetime.today().date())
            return

    if archive_version:
        download_archive(archive_version, data_path)

    else:
        for source_name, source_dict in sources.items():
            if not source_name in ['Energinet.dk', 'ENTSO-E Power Statistics', 'CEPS']:
                download_source(source_name, source_dict, data_path,
                                start_from_user, end_from_user, testmode=testmode)

    return


def download_archive(archive_version, data_path):
    """
    Download archived data from the OPSD server. See download()
    for info on parameter.

    """

    filepath = os.path.join(data_path, 'original_data.zip')

    if not os.path.exists(filepath):
        url = ('http://data.open-power-system-data.org/time_series/'
               '{}/original_data/original_data.zip'.format(archive_version))
        logger.info('Downloading and extracting archived data from %s', url)
        resp = requests.get(url)
        with open(filepath, 'wb') as output_file:
            for chunk in resp.iter_content(1024):
                output_file.write(chunk)

        myzipfile = zipfile.ZipFile(filepath)
        myzipfile.extractall(data_path)
        logger.info('Extracted data to {}'.format(data_path))

    else:
        logger.info('%s already exists. Delete it if you want to download again',
                    filepath)

    return


def download_source(source_name, source_dict, data_path,
                    start_from_user=None, end_from_user=None,
                    testmode=False):
    """
    Download all files for source_name as specified by the given
    source_dict into data_path.

    Parameters
    ----------
    source_name : str
        Name of source dataset, e.g. ``TenneT``.
    source_dict : dict
        Dictionary of variables and their parameters for the given source.
    data_path : str
        Base download directory in which to save all downloaded files.
    start_from_user : datetime.date, default None
        Start of period for which to download the data. 
    end_from_user : datetime.date, default None
        End of period for which to download the data

    Returns
    ----------
    None

    """

    session = None

    for variable_name, param_dict in source_dict.items():
        # Set up the filename structure
        if 'filename' in param_dict:
            filename = param_dict['filename']
        else:
            filename = None

        # Determine time range to be downloaded.
        # Start with everything from first to last datapoint on server
        start_server = param_dict['start']
        end_server = param_dict['end']
        if end_server == 'recent':
            end_server = datetime.now().date()
        # narrow down the time range if specified by user
        if start_from_user:
            if start_from_user <= start_server:
                pass  # do nothing
            elif start_server < start_from_user < end_server:
                start_server = start_from_user  # replace start_server
            else:
                continue  # skip this variable from the source dict, relevant e.g. in Sweden

        if end_from_user:
            if end_from_user <= start_server:
                continue  # skip this variable from the source dict, relevant e.g. in Sweden
            elif start_server < end_from_user < end_server:
                end_server = end_from_user  # replace  end_server
            else:
                pass  # do nothing

        if param_dict['frequency'] in ['complete', 'irregular']:
            # In these two cases, all data is housed in one file on the server
            downloaded, session = download_file(
                source_name,
                variable_name,
                data_path,
                param_dict,
                start=start_server,
                end=end_server,
                filename=filename
            )

        else:
            # In all other cases, the files on the servers usually contain the data for subperiods
            # of some regular length (i.e. months or years available
            # Create lists of start- and enddates of periods represented in
            # individual files to be downloaded.

            # tranlate frequency to argument for pd.date_range()
            freq_start = {'yearly': 'AS', 'quarterly': 'QS',
                          'monthly': 'MS', 'daily': 'D'}
            freq_end = {'yearly': 'A', 'quarterly': 'Q',
                        'monthly': 'M', 'daily': 'D'}

            starts = pd.date_range(
                start=start_server, end=end_server,
                freq=freq_start[param_dict['frequency']]
            )

            ends = pd.date_range(
                start=start_server, end=end_server,
                freq=freq_end[param_dict['frequency']]
            )

            if len(starts) == 0:
                starts = pd.DatetimeIndex([start_server])
            if len(ends) == 0:
                ends = pd.DatetimeIndex([end_server])

            if starts[0].date() > start_server:
                # make sure to include full first period, i.e. if start_server is 2014-12-14,
                # set first start to 2014-01-01
                starts = starts.union([starts[0] - 1])

            if ends[-1].date() < end_server:
                # make sure to include full last period, i.e. if end_server is 2018-01-14,
                # set last end to 2018-01-31
                ends = ends.union([ends[-1] + 1])

            # else:
            #    # extend both by one period to load a little more data than the user asked for.
            #    # Reasoning: The last hour of the year in UTC is already the first hour of the new year in CET
            #    starts = starts.union([starts[-1] + 1])
            #    ends = ends.union([ends[-1] + 1])

            if 'deviant_params' in param_dict:
                for deviant in param_dict['deviant_params']:
                    if start_server <= deviant['start'] <= end_server:
                        downloaded, session = download_file(
                            source_name,
                            variable_name,
                            data_path,
                            param_dict,
                            start=deviant['start'],
                            end=deviant['end'],
                        )

            for s, e in zip(starts, ends):
                downloaded, session = download_file(
                    source_name,
                    variable_name,
                    data_path,
                    param_dict,
                    start=s,
                    end=e,
                    filename=filename,
                    session=session
                )
                if testmode:
                    break

    return


def download_file(
        source_name,
        variable_name,
        data_path,
        param_dict,
        start,
        end,
        filename=None,
        session=None):
    """
    Prepare the Download of a single file.
    Make a directory to save the file to and check if it might have been
    downloaded already

    Parameters
    ----------
    source_name : str
        Name of source dataset, e.g. ``TenneT``
    variable_name : str
        Name of variable, e.g. ``solar``
    data_path : str
        Base download directory in which to save all downloaded files
    param_dict : dict
        Info required for download, e.g. url, url-parameter, filename. 
    start : datetime.date
        start of data in the file
    end : datetime.date
        end of data in the file
    filename : str, default None
        pattern of filename to use if it can not be retrieved from server
    session : requests.session, optional
        If not given, a new session is created.

    Returns
    ----------
    downloaded : bool
        True if download successful, False otherwise.
    session : requests.session

    """
    if session is None:
        session = requests.session()

    message = '| {:20.20} | {:20.20} | {:%Y-%m-%d} | {:%Y-%m-%d} | '.format(
        source_name, variable_name, start, end)

    # Each file will be saved in a folder of its own, this allows us to preserve
    # the original filename when saving to disk.
    container = os.path.join(data_path, source_name, variable_name,
                             start.strftime('%Y-%m-%d') + '_' +
                             end.strftime('%Y-%m-%d'))
    os.makedirs(container, exist_ok=True)

    # Belgian TSO Elia requires start/end with time in UTC format
    if source_name == 'Elia':
        start = (pytz.timezone('Europe/Brussels')
                 .localize(datetime.combine(start, time()))
                 .astimezone(pytz.timezone('UTC')))

        end = (pytz.timezone('Europe/Brussels')
               .localize(datetime.combine(end + timedelta(days=1), time()))
               .astimezone(pytz.timezone('UTC')))

    # Attempt the download if there is no file yet.
    count_files = len(os.listdir(container))
    if count_files == 0:
        if source_name == 'ENTSO-E Transparency FTP':
            downloaded = download_ftp(
                start,
                end,
                filename,
                container,
                address=param_dict['address'],
                user=param_dict['user'],
                passwd=param_dict['passwd'],
                path=param_dict['path']
            )
        else:
            downloaded, session = download_request(
                source_name,
                start,
                end,
                session,
                filename,
                container,
                url_template=param_dict['url_template'],
                url_params_template=param_dict['url_params_template'],
            )
        if downloaded:
            logger.info(message + 'download successful')
        else:
            logger.info(message + 'download failed')

    elif count_files == 1:
        downloaded = True
        logger.debug(message + 'download previously')

    else:
        downloaded = True
        logger.info('There must not be more '
                    'than one file in: %s. Please check ', container)

    return downloaded, session


def download_request(
        source_name,
        start,
        end,
        session,
        filename,
        container,
        url_template,
        url_params_template):
    """
    Download a single file via HTTP get.
    Build the url from parameters and save the file to dsik under it's original
    filename 

    Parameters
    ----------
    container : str
        unique filepath for the file to be saved
    url_template : 
        stem of URL 
    url_params_template : dict
        dict of parameter names and values to paste into URL

    Returns
    ----------
    downloaded : bool
        True if download successful, False otherwise.
    session : requests.session

    """
    url_params = {}  # A dict for URL-parameters

    # For most sources, we can use HTTP get method with parameters-dict
    if url_params_template:
        for key, value in url_params_template.items():
            url_params[key] = value.format(
                u_start=start,
                u_end=end,
            )
        url = url_template

    # For other sources that use urls without parameters (e.g. Svenska
    # Kraftnaet)
    else:
        url = url_template.format(
            u_start=start,
            u_end=end,
        )

    for i in range(10):
        resp = session.get(url, params=url_params)
        if resp.status_code == 200:
            break
        else:
            logger.warning(
                'http status code %s, attempt %s, trying again in 10 seconds...', resp.status_code, i + 1)
            time.sleep(70)
            if i == 9:
                downloaded = False
                return downloaded, session

    # Get the original filename
    try:
        original_filename = (
            resp.headers['content-disposition']
            .split('filename=')[-1]
            .replace('"', '')
            .replace(';', '')
        )
        logger.debug('Downloaded from URL: %s Original filename: %s',
                     resp.url, original_filename)

    # For cases where the original filename can not be retrieved,
    # I put the filename in the param_dict
    except KeyError:
        if filename:
            original_filename = filename.format(u_start=start, u_end=end)
        else:
            logger.info(
                'original filename could neither be retrieved from server '
                'nor sources.yml'
            )
            original_filename = 'unknown_filename'

        logger.debug('Downloaded from URL: %s', resp.url)

    # Save file to disk
    filepath = os.path.join(container, original_filename)
    with open(filepath, 'wb') as output_file:
        for chunk in resp.iter_content(1024):
            output_file.write(chunk)
    downloaded = True
    return downloaded, session


def download_ftp(
        start,
        end,
        filename,
        container,
        address,
        user,
        passwd,
        path):
    """
    Download a single file via FTP.

    Parameters
    ----------
    container : str
        unique filepath for the file to be saved
    address : 
        Server address
    user : str
        username
    passwd : str
        password
    path : 
        directory on server

    Returns
    ----------
    downloaded : bool
        True if download successful, False otherwise.

    """

    ftp = FTP(host=address, user=user, passwd=passwd)
    ftp.cwd(path)
    filename = filename.format(u_start=start, u_end=end)
    filepath = os.path.join(container, filename)
    #filesize = ftp.size(filename)

    # Retrieve file and save to disk
    #downloadTracker = FtpDownloadTracker(ftp, filesize, filepath)
    ftp.retrbinary('RETR ' + filename, callback=open(filepath, 'wb').write,  # callback=downloadTracker.handle,
                   blocksize=1024)

    downloaded = True
    return downloaded


class FtpDownloadTracker:
    """
    Object that provides the path to save a file from FTP locally together
    with a method to track the download progress via a handle for the
    ftplib.FTP.retrbinary method.
    """

    sizeWritten = 0
    totalSize = 0
    lastShownPercent = 0

    def __init__(self, ftp, filesize, filepath):
        self.totalSize = filesize
        self.filepath = filepath

    def handle(self, block):
        with open(self.filepath, 'wb') as output_file:
            output_file.write(block)

        self.sizeWritten += 1024
        progress = round(self.sizeWritten / self.totalSize, 2)

        if (self.lastShownPercent != progress):
            self.lastShownPercent = progress
            update_progress(progress, self.totalSize)


def update_progress(progress, total):
    '''
    Display or updates a console progress bar.

    Parameters
    ----------
    progress : float
        fraction of file already downloades 
    total : int
        total number of files

    Returns
    ----------
    None

    '''

    barLength = 50  # Modify this to change the length of the progress bar
    status = ""
    block = int(round(barLength * progress))
    text = "\rProgress: [{0}] {1:.0%} of {2} {3}".format(
        "#" * block + "-" * (barLength - block),
        progress, convert_size(total), status)
    sys.stdout.write(text)
    sys.stdout.flush()

    return


def convert_size(size_bytes):
    '''
    Convert byte to kilobyte, megabyte etc.

    Parameters
    ----------
    size_bytes : int
        filesize in byte

    Returns
    ----------
    str
        filesize in KB/MB/etc.

    '''

    if (size_bytes == 0):
        return '0B'
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return '{} {}'.format(s, size_name[i])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('sources_yaml_path', type=str)
    parser.add_argument('data_path', type=str)
    parser.add_argument('end_from_user', type=date)
    parser.add_argument('-s', '--subset', nargs='*', action='append')
    args = parser.parse_args()

   # download(args.sources_yaml_path, args.data_path, args.subset)
