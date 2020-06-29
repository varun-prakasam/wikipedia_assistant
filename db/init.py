import os
import logging

from db.maria import MariaDBConnector
from etl.constants import ROOT_DIR, PROJECT_NAME, PROD_TABLE_NAMES, ETL_JOB_LOG


def init_database(init_sql_script_path=None):
    if not init_sql_script_path:
        init_sql_script_path = os.path.join(ROOT_DIR, PROJECT_NAME, 'db/sql/initialize_wiki_db.sql')

    if not os.path.exists(init_sql_script_path):
        raise FileNotFoundError(f'Could not find the init sql script @ {init_sql_script_path}')

    con = MariaDBConnector()
    con.source_sql_script(sql_script_path=init_sql_script_path)

    r1 = con.execute(f'SELECT * FROM {PROD_TABLE_NAMES}')
    r2 = con.execute(f'SELECT * FROM {ETL_JOB_LOG}')

    assert len(r1) > 0
    assert len(r2) == 0
    logging.info(f'Tables that are present in production and are part of ETL job:\n{r1}\n')
    logging.info('Database has been initialized successfully')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    init_database(os.path.join('/Users/varunprakasam/PycharmProjects/wikipedia_assistant/db/sql/',
                               'initialize_wiki_db.sql'))
