import os
import gzip
import shutil
import logging


def extract_gzip_file(gzip_file_path, output_path=None, delete_existing=False):
    if not gzip_file_path.endswith('.gz'):
        raise RuntimeError('Only extraction of gzip files is supported')

    if output_path is None:
        output_path = gzip_file_path.rstrip('.gz')

    if os.path.exists(output_path):
        if delete_existing:
            os.unlink(output_path)
        else:
            raise RuntimeError(f'File already exists at {output_path}. Please provide a different output path.')

    logging.info(f'Extracting {gzip_file_path} to {output_path}')
    with gzip.open(gzip_file_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    logging.info('Extraction complete\n')
