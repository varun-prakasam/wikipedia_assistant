import os
import time
import logging

from db.maria import MariaDBConnector
from etl.constants import ROOT_DIR, PROJECT_NAME, MOST_OUTDATED_PAGE


class DBPreProcessor:

    def __init__(self, sql_script_path_mo=None):
        if sql_script_path_mo is None:
            self.sql_script_path_mo = os.path.join(ROOT_DIR, PROJECT_NAME, 'etl/sql/pre_processing_most_outdated.sql')
        else:
            self.sql_script_path_mo = sql_script_path_mo
        if not os.path.exists(self.sql_script_path_mo):
            raise FileNotFoundError(f'Could not find sql script @ {self.sql_script_path_mo}')

        self.most_outdated_page_table_name = MOST_OUTDATED_PAGE
        self.category_links_table_name = 'wikipedia.categorylinks'
        self.con = MariaDBConnector()

    def run_most_outdated_pre_processing(self):
        start_time = time.time()
        self.con.source_sql_script(sql_script_path=self.sql_script_path_mo)
        end_time = time.time()

        rc1 = self.con.execute(f'SELECT COUNT(*) FROM {self.most_outdated_page_table_name}', as_df=False)
        assert rc1[0][0] > 0

        rc2 = self.con.execute(f'SELECT COUNT(DISTINCT cl_to) FROM {self.category_links_table_name}', as_df=False)
        assert rc2[0][0] > 0
        assert rc1[0][0] > rc2[0][0] // 2

        logging.info(f'Most outdated pre-processing complete in {round(end_time - start_time, 1)} s')

    def run_pre_processing(self):
        self.run_most_outdated_pre_processing()
        logging.info('All pre-processing complete')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    f_path = '/Users/varunprakasam/PycharmProjects/wikipedia_assistant/etl/sql/pre_processing_most_outdated.sql'
    db_pre_processor = DBPreProcessor(sql_script_path_mo=f_path)
    db_pre_processor.run_pre_processing()
