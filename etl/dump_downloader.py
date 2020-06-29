import os
import logging
import time

import requests
import datetime as dt
from functools import lru_cache

from db.maria import MariaDBConnector
from etl.utils import extract_gzip_file, get_simple_wiki_url, extract_bz2_file
from etl.constants import DUMP_DOWNLOAD_DIR, PROD_TABLE_NAMES


class DumpDownloader:

    def __init__(self, dump_date):
        self.dump_date = dump_date
        self.download_dir = os.path.join(DUMP_DOWNLOAD_DIR, dt.datetime.strftime(self.dump_date, '%Y%m%d'))
        self.all_tables = {}

        con = MariaDBConnector()
        res = con.execute(f'SELECT * FROM {PROD_TABLE_NAMES}', as_df=False)

        for table_name, filename_format in res:
            self.all_tables[table_name] = filename_format

    @property
    def tables(self):
        return list(self.all_tables.keys())

    @lru_cache()
    def get_filename(self, table):
        if table not in self.all_tables:
            raise RuntimeError(f'Table {table} is not valid. The list of valid tables are: {self.tables}')
        return self.all_tables[table].format(YYYYMMDD=dt.datetime.strftime(self.dump_date, '%Y%m%d'))

    @lru_cache()
    def get_download_url(self, table):
        base_url = get_simple_wiki_url(date=self.dump_date)
        return base_url + self.get_filename(table=table)

    def get_download_to_path(self, table):
        return os.path.join(self.download_dir, self.get_filename(table=table))

    def download_table(self, table):
        url = self.get_download_url(table=table)
        download_to_path = self.get_download_to_path(table=table)

        # Check if directory exists and create if it doesn't
        download_dir = os.path.dirname(download_to_path)
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        # If file already exists at download path, delete it first
        if os.path.exists(download_to_path):
            logging.info(f'Deleting file that already exists at download path {download_to_path}')
            os.unlink(download_to_path)

        logging.info(f'Downloading {table} from {url} to {download_to_path}...')
        resp = requests.get(url, allow_redirects=True)

        if resp.status_code != 200:
            raise RuntimeError(f'HTTP error when downloading from {url}: status_code={resp.status_code}')

        with open(download_to_path, 'wb') as f:
            f.write(resp.content)
        logging.info(f'Download complete for {table}')

        if download_to_path.endswith('.gz'):
            final_path = extract_gzip_file(gzip_file_path=download_to_path, delete_existing=True)
        elif download_to_path.endswith('.bz2'):
            final_path = extract_bz2_file(bz2_file_path=download_to_path, delete_existing=True)
        else:
            final_path = download_to_path

        logging.info(f'Dump download for {table} complete\n')
        return final_path

    def download_dump(self):
        download_paths = {}
        logging.info(f'Downloading dumps for following tables: {list(self.tables)}')
        t1 = time.time()

        for table in self.tables:
            download_path = self.download_table(table=table)
            download_paths[table] = download_path

        t2 = time.time()
        tt = round(t2 - t1, 1)
        logging.info(f"Total {len(self.tables)} tables' dumps downloaded for dump date {self.dump_date} in {tt} s\n")
        return download_paths


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    downloader = DumpDownloader(dump_date=dt.date(2020, 6, 20))
    ret = downloader.download_dump()
    print(ret)
