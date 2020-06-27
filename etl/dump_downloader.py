import os
import logging
import requests
import datetime as dt
from etl.utils import extract_gzip_file
from etl.constants import DUMP_DOWNLOAD_DIR


class DumpDownloader:

    def __init__(self, dump_date=dt.date(2020, 6, 20)):
        self.dump_date = dump_date
        self.download_dir = os.path.join(DUMP_DOWNLOAD_DIR, dt.datetime.strftime(self.dump_date, '%Y%m%d'))

        url = 'https://dumps.wikimedia.org/simplewiki/20200620/simplewiki-20200620-categorylinks.sql.gz'
        self.download_urls = {'categorylinks': url}

    @property
    def tables(self):
        return self.download_urls.keys()

    def get_download_url(self, table):
        return self.download_urls[table]

    def get_download_to_path(self, table):
        filename = self.download_urls[table].split('/')[-1]
        return os.path.join(self.download_dir, filename)

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

        logging.info(f'Downloading {table} to {download_to_path}...')
        resp = requests.get(url, allow_redirects=True)

        with open(download_to_path, 'wb') as f:
            f.write(resp.content)
        logging.info(f'Download complete for {table}')

        if download_to_path.endswith('.gz'):
            extract_gzip_file(gzip_file_path=download_to_path, delete_existing=True)

    def download_dump(self):
        for table in self.tables:
            self.download_table(table=table)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    downloader = DumpDownloader()
    downloader.download_dump()
