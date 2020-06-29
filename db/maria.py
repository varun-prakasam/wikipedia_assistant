import os
import logging
import time

import pymysql
import pandas as pd
import datetime as dt
from subprocess import Popen, PIPE
from etl.constants import TMP_FILES_DIR
from db.credentials import CredentialsProvider


class MariaDBConnector:

    def __init__(self):
        self.credentials = CredentialsProvider.get_maria_db_credentials()

    def execute(self, query, as_df=True, verbose=True):
        db = pymysql.connect(
            self.credentials['host'],
            self.credentials['user'],
            self.credentials['password'],
            self.credentials['database']
        )

        cursor = db.cursor()
        if verbose:
            logging.info(f'Executing: {query}')

        try:
            cursor.execute(query)
            db.commit()
        except:
            db.rollback()

        data = cursor.fetchall()
        if len(data) > 0 or (cursor.description and len(cursor.description) > 0):
            if as_df:
                try:
                    res = pd.DataFrame(data, columns=[c[0] for c in cursor.description])
                except:
                    res = data
            else:
                res = data
        else:
            res = cursor.rowcount

        db.close()
        return res

    def upload_df(self, df, table_name, delimiter=',', name=''):
        if len(df) == 0:
            logging.info('Dataframe is empty. Nothing to load.')
            return

        now = dt.datetime.strftime(dt.datetime.today(), '%Y%m%d_%H%M%S')
        csv_path = os.path.join(TMP_FILES_DIR, f'{now}.csv')

        if not os.path.exists(TMP_FILES_DIR):
            os.makedirs(TMP_FILES_DIR)
        if os.path.exists(csv_path):
            os.unlink(csv_path)

        name = name + " " if name else name
        logging.info(f'Loading {name}dataframe of shape ({df.shape[0]}, {df.shape[1]}) to {table_name}')
        df.to_csv(csv_path, sep=delimiter, index=False)
        self.upload_csv(csv_path=csv_path, table_name=table_name, delimiter=delimiter)

    def upload_csv(self, csv_path, table_name, delimiter=',', delete_csv_after=True, ignore_header=True):
        cmd = f"LOAD DATA LOCAL INFILE '{csv_path}' INTO TABLE {table_name} FIELDS TERMINATED BY '{delimiter}'"
        if ignore_header:
            cmd += ' IGNORE 1 LINES;'
        else:
            cmd += ';'

        self.run_cmd_on_cli(cmd=cmd)
        if delete_csv_after:
            os.unlink(csv_path)

    def source_sql_script(self, sql_script_path):
        if not sql_script_path.endswith('.sql'):
            raise RuntimeError(f'{sql_script_path} is not a .sql file')
        if not os.path.exists(sql_script_path):
            raise FileNotFoundError(sql_script_path)

        cmd = f'source {sql_script_path};'
        self.run_cmd_on_cli(cmd=cmd)

    def run_cmd_on_cli(self, cmd, verbose=False):
        assert cmd.endswith(';')
        process = Popen(['mysql',
                         f'--host={self.credentials["host"]}',
                         f'--user={self.credentials["user"]}',
                         f'--password={self.credentials["password"]}',
                         self.credentials['database']],
                        stdout=PIPE,
                        stdin=PIPE,
                        encoding='utf8')

        logging.info(f'Executing command: {cmd}')
        output = process.communicate(cmd)

        time.sleep(3)
        if output[0] and verbose:
            print(output[0])
        process.terminate()
        logging.info('Finished executing command on mysql cli')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    con = MariaDBConnector()
    ret = con.execute('SELECT * FROM wikipedia_etl.table_names_production')
    print(ret)
