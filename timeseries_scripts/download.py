"""
Open Power System Data

Timeseries Datapackage

download.py : download time series files

"""

import argparse
from datetime import datetime, date
import getpass
import logging
import os

import pandas as pd
import requests
import yaml


def get_opsd_beta_password():
    if 'MORPH_OPSD_BETA_PW' in os.environ:
        password = os.environ['MORPH_OPSD_BETA_PW']
    else:
        password = getpass.getpass('Please enter the beta user password:')
    return password


def download_file(source_name, variable_name, out_path,
                  param_dict, start, end, session=None):
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
        blubb
    start :
        blubb
    end :
        blubb
    session : requests.session, optional
        If not given, a new session is created.

    """
    if session is None:
        session = requests.session()

    logging.info(
        'Downloading: {} {} {:%Y-%m-%d}_{:%Y-%m-%d}'
        .format(source_name, variable_name, start, end)
    )

    # Get number of months between now and start (required for TransnetBW).
    count = (datetime.now().month
             - start.month
             + (datetime.now().year - start.year) * 12)

    # Create the parameters dict containing timespan info to be pasted with url
    url_params = {}
    if param_dict['url_params_template']:
        for key, value in param_dict['url_params_template'].items():
            url_params[key] = value.format(u_start=start,
                                           u_end=end,
                                           u_transnetbw=count)
        url = param_dict['url_template']
    else:
        url = param_dict['url_template'].format(u_start=start,
                                                u_end=end,
                                                u_transnetbw=count)        

    # Each file will be saved in a folder of its own, this allows us to preserve
    # the original filename when saving to disk.
    container = os.path.join(out_path, source_name, variable_name,
                             start.strftime('%Y-%m-%d') + '_' +
                             end.strftime('%Y-%m-%d'))
    os.makedirs(container, exist_ok=True)

    # Attempt the download if there is no file yet.
    count_files = len(os.listdir(container))
    if count_files == 0:
        resp = session.get(url, params=url_params)

        if 'filename' in param_dict:
            original_filename = param_dict['filename']
        else:
            original_filename = (resp.headers['content-disposition']
                                     .split('filename=')[-1]
                                     .replace('"', '')
                                     .replace(';', '')
                                 )

        logging.info('Downloaded from URL: %s Original filename: %s',
                     resp.url, original_filename)

        filepath = os.path.join(container, original_filename)
        with open(filepath, 'wb') as output_file:
            for chunk in resp.iter_content(1024):
                output_file.write(chunk)

    elif count_files == 1:
        logging.info('There is already a file: %s', os.listdir(container)[0])

    else:
        logging.info('There must not be more than one file in: %s. Please check ',
                     container)


def download_source(source_name, source_dict, out_path, start_date=None):
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
    start_date : datetime.date, optional
        Override the start date from source_dict,
        e.g.: start_date=date(2015,1,1)

    """

    # While OPSD is in beta, we need to supply authentication
    if source_name == 'OPSD':
        password = get_opsd_beta_password()
        session = requests.session()
        session.auth = ('beta', password)
    else:
        session = None

    for variable_name, param_dict in source_dict.items():
        if start_date:
            param_dict['start'] = start_date

        if param_dict['end'] == 'recent':
            param_dict['end'] = date(2015, 12, 31)

        if param_dict['frequency'] == 'complete':
            download_file(
                source_name, variable_name, out_path, param_dict,
                start=param_dict['start'], end=param_dict['end'],
                session=session
            )
        else:
            # The files on the servers usually contain the data for subperiods
            # of some regular length (i.e. months or yearsavailable
            # Create lists of start- and enddates of periods represented in
            # individual files to be downloaded.
            starts = pd.date_range(
                start=param_dict['start'], end=param_dict['end'],
                freq=param_dict['frequency'] + 'S'
            )
            ends = pd.date_range(
                start=param_dict['start'], end=param_dict['end'],
                freq=param_dict['frequency']
            )
            for s, e in zip(starts, ends):
                download_file(
                    source_name, variable_name, out_path, param_dict,
                    start=s, end=e, session=session
                )


def download(sources_yaml_path, out_path, subset=None):
    """
    Load YAML file with sources from disk, and download all files for each
    source into the given out_path. Returns None.

    Parameters
    ----------
    subset : list or iterable, optional
        If given, specifies a subset of data sources to download,
        e.g.: ['TenneT', '50Hertz'].

    """
    with open(sources_yaml_path, 'r') as f:
        sources = yaml.load(f.read())

    # If subset is given, only keep source_name keys in subset
    if subset is not None:
        sources = {k: v for k, v in sources.items() if k in subset}

    for source_name, source_dict in sources.items():
        download_source(source_name, source_dict, out_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('sources_yaml_path', type=str)
    parser.add_argument('out_path', type=str)
    parser.add_argument('-s', '--subset', nargs='*', action='append')
    args = parser.parse_args()

    download(args.sources_yaml_path, args.out_path, args.subset)
