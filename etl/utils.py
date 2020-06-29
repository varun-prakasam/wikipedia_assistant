import os
import bz2
import gzip
import shutil
import logging
import requests
import datetime as dt
from dateutil.relativedelta import relativedelta as rd

from etl.constants import SIMPLE_WIKI_BASE_URL


def extract_gzip_file(gzip_file_path, output_path=None, delete_existing=False):
    if not gzip_file_path.endswith('.gz'):
        raise RuntimeError('Only extraction of gzip files is supported')

    if output_path is None:
        output_path = gzip_file_path.rstrip('.gz')

    if os.path.exists(output_path):
        if delete_existing:
            logging.info(f'Deleting file that already exists at extract path {output_path}')
            os.unlink(output_path)
        else:
            raise RuntimeError(f'File already exists at {output_path}. Please provide a different output path.')

    logging.info(f'Extracting {gzip_file_path} to {output_path}')
    with gzip.open(gzip_file_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    logging.info('Extraction of gzip file complete')
    return output_path


def extract_bz2_file(bz2_file_path, output_path=None, delete_existing=False):
    if not bz2_file_path.endswith('.bz2'):
        raise RuntimeError('Only extraction of bz2 files is supported')

    if output_path is None:
        output_path = bz2_file_path.rstrip('.bz2')

    if os.path.exists(output_path):
        if delete_existing:
            logging.info(f'Deleting file that already exists at extract path {output_path}')
            os.unlink(output_path)
        else:
            raise RuntimeError(f'File already exists at {output_path}. Please provide a different output path.')

    logging.info(f'Extracting {bz2_file_path} to {output_path}')
    with bz2.open(bz2_file_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    logging.info('Extraction of bz2 file complete')
    return output_path


def replace_string_in_file(file_path, old_str, new_str, out_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)
    if os.path.exists(out_path):
        os.unlink(out_path)

    fin = open(file_path, 'rt')
    data = fin.read()
    data = data.replace(old_str, new_str)
    fin.close()

    fout = open(out_path, 'wt')
    fout.write(data)
    fout.close()


def get_simple_wiki_latest_dump_date():
    curr_date = dt.datetime.now().date()
    curr_url = get_simple_wiki_url(curr_date)
    resp = requests.get(curr_url)

    if resp.status_code != 200:
        while resp.status_code != 200:
            curr_date -= rd(days=1)
            curr_url = get_simple_wiki_url(curr_date)
            resp = requests.get(curr_url)

        return curr_date

    else:
        while resp.status_code == 200:
            curr_date += rd(days=1)
            curr_url = get_simple_wiki_url(curr_date)
            resp = requests.get(curr_url)

        return curr_date - rd(days=1)


def get_simple_wiki_url(date):
    return SIMPLE_WIKI_BASE_URL.format(YYYYMMDD=dt.datetime.strftime(date, '%Y%m%d'))
