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

import pandas as pd
import requests
import yaml

logger = logging.getLogger('log')
logger.setLevel('INFO')


def download(sources_yaml_path, out_path, start_from_user=None, end_from_user=None, subset=None):
    """
    Load YAML file with sources from disk, and download all files for each
    source into the given out_path. Returns None.

    Parameters
    ----------
    sources_yaml_path : str
        Filepath of sources.yml
    out_path : str
        Base download directory in which to save all downloaded files.
    start_from_user : datetime.date, optional
        Start of period for which to download the data.
    end_from_user : datetime.date, optional
        End of period for which to download the data
    subset : list or iterable, optional
        If given, specifies a subset of data sources to download,
        e.g.: ['TenneT', '50Hertz'].

    """
    for name, date in {'end_from_user': end_from_user, 'start_from_user': start_from_user}.items():
        if date and date > datetime.now().date():
            logger.info('%s given was %s, must be smaller than %s, '
                        'we have no data for the future!',
                        name, date, datetime.today().date())
            return

    with open(sources_yaml_path, 'r') as f:
        sources = yaml.load(f.read())

    # If subset is given, only keep source_name keys in subset
    if subset is not None:
        sources = {k: v for k, v in sources.items() if k in subset}

    for source_name, source_dict in sources.items():
        download_source(source_name, source_dict, out_path, start_from_user, end_from_user)


def download_source(source_name, source_dict, out_path, start_from_user=None, end_from_user=None):
    """
    Download all files for source_name as specified by the given
    source_dict into out_path. Returns None.

    Parameters
    ----------
    source_name : str
        Name of source dataset, e.g. ``TenneT``.
    source_dict : dict
        Dictionary of variables and their parameters for the given source.
    out_path : str
        Base download directory in which to save all downloaded files.
    start_from_user : datetime.date, optional
        Start of period for which to download the data.
    end_from_user : datetime.date, optional
        End of period for which to download the data

    """

    for variable_name, param_dict in source_dict.items():
        start_server = param_dict['start']
        end_server = param_dict['end']

        if 'filename' in param_dict:
            filename = param_dict['filename']
        else:
            filename = None

        if end_server == 'recent':
            end_server = datetime.now().date()

        if start_from_user:
            if start_from_user <= start_server:
                pass  # do nothing
            # elif start_from_user > param_dict['start'] and start_from_user < param_dict['end']:
            elif start_server < start_from_user < end_server:
                start_server = start_from_user  # replace start_server
            else:
                continue  # skip this variable from the source dict, relevant e.g. in Sweden

        if end_from_user:
            if end_from_user <= start_server:
                continue  # skip this variable from the source dict, relevant e.g. in Sweden
            # elif end_from_user > param_dict['start'] and end_from_user < param_dict['end']:
            elif start_server < end_from_user < end_server:
                end_server = end_from_user  # replace  end_server
            else:
                pass  # do nothing

        if param_dict['frequency'] in ['complete', 'irregular']:
            download_file(
                source_name,
                variable_name,
                out_path,
                #param_dict,
                start=start_server,
                end=end_server,
                url_template=param_dict['url_template'],
                url_params_template=param_dict['url_params_template'],
                filename=filename
            )

        else:
            # The files on the servers usually contain the data for subperiods
            # of some regular length (i.e. months or yearsavailable
            # Create lists of start- and enddates of periods represented in
            # individual files to be downloaded.

            # tranlate frequency to argument for pd.date_range()
            freq_start = {'yearly': 'AS', 'monthly': 'MS', 'daily': 'D'}
            freq_end = {'yearly': 'A', 'monthly': 'M', 'daily': 'D'}

            starts = pd.date_range(
                start=start_server, end=end_server,
                freq=freq_start[param_dict['frequency']]
            )
            ends = pd.date_range(
                start=start_server, end=end_server,
                freq=freq_end[param_dict['frequency']]
            )

            if len(ends) == 0:
                ends = pd.DatetimeIndex([end_server])

            if 'deviant_urls' in param_dict:
                for deviating in param_dict['deviant_urls']:
                    download_file(
                        source_name,
                        variable_name,
                        out_path,
                        # param_dict,
                        start=deviating['start'],
                        end=deviating['end'],
                        url_template=deviating['url']
                    )

            for s, e in zip(starts, ends):

                # The Polish TSO PSE has daily files that are usually uploaded
                # 6 days later somtime between 17:00:10 and 17:01:30. As the exact
                # second needs is unknown ex-ante, but needs to be included in the URL,
                # we need to try out every second in that period until the file is
                # found.
                if source_name == 'PSE':
                    downloaded = False
                    for second in pd.date_range(
                            start=datetime.combine(s + timedelta(days=6), time(17, 0, 10)),
                            end=datetime.combine(s + timedelta(days=6), time(17, 2, 0)),
                            freq='S'):
                        if not downloaded:
                            logger.info('test %s', second)
                            downloaded = download_file_pse(
                                source_name,
                                variable_name,
                                out_path,
                                # param_dict,
                                start=s,
                                end=e,
                                url_template=param_dict['url_template'],
                                url_params_template=param_dict['url_params_template'],
                                second=second
                            )

                else:
                    download_file(
                        source_name,
                        variable_name,
                        out_path,
                        # param_dict,
                        start=s,
                        end=e,
                        url_template=param_dict['url_template'],
                        url_params_template=param_dict['url_params_template'],
                        filename=filename
                    )

    return


def download_file_pse(
    source_name,
    variable_name,
    out_path,
     #param_dict,
    start,
    end,
    url_template,
    url_params_template=None,
    filename=None,
    session=None,
    second=None):
    """
    Download a single file specified by ``param_dict``, ``start``, ``end``,
    and save it to a directory constructed by combining ``source_name``,
    ``variable_name`` and ``out_path``. Returns None.

    Parameters
    ----------
    source_name : str
        Name of source dataset, e.g. ``TenneT``
    variable_name : str
        Name of variable, e.g. ``solar``
    out_path : str
        Base download directory in which to save all downloaded files
    param_dict : dict
        Info required for download, e.g. url, url-parameter, filename.
    start : datetime.date
        start of data in the file
    end : datetime.date
        end of data in the file
    session : requests.session, optional
        If not given, a new session is created.

    """
    if session is None:
        session = requests.session()

    # Each file will be saved in a folder of its own, this allows us to preserve
    # the original filename when saving to disk.
    container = os.path.join(out_path, source_name, variable_name,
                             start.strftime('%Y-%m-%d') + '_' +
                             end.strftime('%Y-%m-%d'))
    os.makedirs(container, exist_ok=True)

    url_params = {}  # A dict for URL-parameters
    # For most sources, we can use HTTP get method with parameters-dict
    if url_params_template:
        for key, value in url_params_template.items():
            url_params[key] = value.format(
                u_start=start,
                u_end=end,
                u_second=second
            )
        url = url_template

    # Attempt the download if there is no file yet.
    count_files = len(os.listdir(container))
    if count_files == 0:
        resp = session.get(url, params=url_params)
        if not resp.text == 'Brak uprawnieñ':
            logger.info(
                'Downloaded data:\n\t '
                'Source:      {}\n\t '
                'Variable:    {}\n\t '
                'Data starts: {:%Y-%m-%d}\n\t '
                'Data ends:   {:%Y-%m-%d}'
                .format(source_name, variable_name, start, end)
            )

            # Get the original filename
            original_filename = (
                resp.headers['content-disposition']
                .split('filename=')[-1]
                .replace('"', '')
                .replace(';', '')
            )
            logger.info('Downloaded from URL: %s\n\t Original filename: %s',
                        resp.url, original_filename)

            # Save file to disk
            filepath = os.path.join(container, original_filename)
            with open(filepath, 'wb') as output_file:
                for chunk in resp.iter_content(1024):
                    output_file.write(chunk)
            downloaded = True

        else:
            downloaded = False

    elif count_files == 1:
        downloaded = True
        logger.info('Found local file: %s', os.listdir(container)[0])

    else:
        downloaded = True
        logger.info('There must not be more '
                    'than one file in: %s. Please check.', container)

    return downloaded


def download_file(
    source_name,
    variable_name,
    out_path,
    # param_dict,
    start,
    end,
    url_template,
    url_params_template=None,
    filename=None,
    session=None,
    second=None
    ):
    """
    Download a single file specified by ``param_dict``, ``start``, ``end``,
    and save it to a directory constructed by combining ``source_name``,
    ``variable_name`` and ``out_path``. Returns None.

    Parameters
    ----------
    source_name : str
        Name of source dataset, e.g. ``TenneT``
    variable_name : str
        Name of variable, e.g. ``solar``
    out_path : str
        Base download directory in which to save all downloaded files
    param_dict : dict
        Info required for download, e.g. url, url-parameter, filename.
    start : datetime.date
        start of data in the file
    end : datetime.date
        end of data in the file
    session : requests.session, optional
        If not given, a new session is created.

    """
    if session is None:
        session = requests.session()

    logger.info(
        'Downloading data:\n\t '
        'Source:      {}\n\t '
        'Variable:    {}\n\t '
        'Data starts: {:%Y-%m-%d}\n\t '
        'Data ends:   {:%Y-%m-%d}'
        .format(source_name, variable_name, start, end)
    )

    # Each file will be saved in a folder of its own, this allows us to preserve
    # the original filename when saving to disk.
    container = os.path.join(out_path, source_name, variable_name,
                             start.strftime('%Y-%m-%d') + '_' +
                             end.strftime('%Y-%m-%d'))
    os.makedirs(container, exist_ok=True)

    # Get number of months between now and start (required for TransnetBW).
    count = (datetime.now().month
             - start.month
             + (datetime.now().year - start.year) * 12)

    # Belgian TSO Elia requires start/end with time in UTC format
    if source_name == 'Elia':
        start = (pytz.timezone('Europe/Brussels')
                 .localize(datetime.combine(start, time()))
                 .astimezone(pytz.timezone('UTC')))

        end = (pytz.timezone('Europe/Brussels')
               .localize(datetime.combine(end+timedelta(days=1), time()))
               .astimezone(pytz.timezone('UTC')))

    url_params = {}  # A dict for URL-parameters

    # For most sources, we can use HTTP get method with parameters-dict
    if url_params_template:
        for key, value in url_params_template.items():
            url_params[key] = value.format(
                u_start=start,
                u_end=end,
                u_transnetbw=count,
                u_second=second
            )
        url = url_template

    # For other sources that use urls without parameters (e.g. Svenska Kraftnaet)
    else:
        url = url_template.format(
            u_start=start,
            u_end=end,
            u_transnetbw=count,
            u_second=second
        )

    # Attempt the download if there is no file yet.
    count_files = len(os.listdir(container))
    if count_files == 0:
        resp = session.get(url, params=url_params)

        # Get the original filename
        try:
            original_filename = (
                resp.headers['content-disposition']
                .split('filename=')[-1]
                .replace('"', '')
                .replace(';', '')
            )
            logger.info('Downloaded from URL: %s\n\t Original filename: %s',
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

            logger.info('Downloaded from URL: %s', resp.url)

        # Save file to disk
        if not resp.text == 'Brak uprawnieñ':
            filepath = os.path.join(container, original_filename)
            with open(filepath, 'wb') as output_file:
                for chunk in resp.iter_content(1024):
                    output_file.write(chunk)
            downloaded = True

        else:
            downloaded = False

    elif count_files == 1:
        downloaded = True
        logger.info('Found local file: %s', os.listdir(container)[0])

    else:
        downloaded = True
        logger.info('There must not be more '
                    'than one file in: %s. Please check.', container)

    return downloaded


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('sources_yaml_path', type=str)
    parser.add_argument('out_path', type=str)
    parser.add_argument('end_from_user', type=date)
    parser.add_argument('-s', '--subset', nargs='*', action='append')
    args = parser.parse_args()

    download(args.sources_yaml_path, args.out_path, args.subset)
