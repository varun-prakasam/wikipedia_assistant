import time
import logging
import datetime as dt

from db.maria import MariaDBConnector
from etl.constants import ETL_JOB_LOG
from etl.xml_parser import WikiXMLParser
from etl.dump_downloader import DumpDownloader
from etl.db_pre_processor import DBPreProcessor
from etl.utils import get_simple_wiki_latest_dump_date, get_latest_loaded_date


def get_etl_job_id():
    con = MariaDBConnector()
    df = con.execute(query=f'SELECT MAX(job_id) AS max_job_id FROM {ETL_JOB_LOG}')
    if len(df) == 0 or df.max_job_id[0] is None:
        return 1
    else:
        return df.max_job_id[0] + 1


def main(wiki_dump_date=None, fail_upon_errors=True):
    etl_job_id = get_etl_job_id()
    logging.info(f'Commencing ETL job for Wikipedia Assistant with job id {etl_job_id}')
    start_time = time.time()

    if wiki_dump_date is None:
        dump_date = get_simple_wiki_latest_dump_date()
        latest_loaded_date = get_latest_loaded_date()

        if dump_date <= latest_loaded_date:
            logging.info(f'Latest dump date from wiki is {dump_date} and latest loaded date is {latest_loaded_date}')
            logging.info('So not running ETL routine since there is no new dump available')
            return
    else:
        dump_date = wiki_dump_date

    logging.info(f'Downloading all latest data dumps from Simple Wiki for dump date {dump_date}\n')
    downloader = DumpDownloader(dump_date=dump_date)
    download_paths = downloader.download_dump()

    con = MariaDBConnector()
    error_files = []
    logging.info(f'Loading data from dumps to database for following tables: {download_paths.keys()}')

    for table, file_path in download_paths.items():
        t1 = dt.datetime.now()
        logging.info(f'Loading to {table} table')

        if file_path.endswith('.sql'):
            con.source_sql_script(sql_script_path=file_path)

        elif file_path.endswith('.xml') and 'pages-meta-current' in file_path:
            xml_parser = WikiXMLParser(xml_path=file_path)
            xml_parser.parse_and_load_all_pages_xml(batch_size=10000)

        else:
            logging.warning(f'File {file_path} is not supported for ETL to DB')
            error_files.append((table, file_path, 'unsupported'))

        t2 = dt.datetime.now()
        t1_str = dt.datetime.strftime(t1, '%Y-%m-%dT%H:%M:%S')
        t2_str = dt.datetime.strftime(t2, '%Y-%m-%dT%H:%M:%S')

        con.execute(query=f"INSERT INTO {ETL_JOB_LOG} VALUES "
                          f"({etl_job_id}, '{dump_date}', '{table}', '{file_path}', '{t1_str}', '{t2_str}')")
        logging.info(f'Loaded to {table} table in {(t2 - t1).seconds} s\n')
        time.sleep(2)

    logging.info('Starting pre-processing of database to aid most outdated page queries')
    db_pre_processor = DBPreProcessor()
    db_pre_processor.run_pre_processing()

    end_time = time.time()
    mins_taken = (end_time - start_time) // 60
    secs_taken = round((end_time - start_time) % 60, 1)
    logging.info(f'Finished end-to-end ETL for {len(download_paths)} tables in {mins_taken} min {secs_taken} s')

    if error_files:
        if fail_upon_errors:
            raise RuntimeError(f'There were following errors when loading to these tables: {error_files}')
        else:
            logging.warning(f'There were following errors when loading to these tables: {error_files}')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
