import os

PROJECT_NAME = 'wikipedia_assistant'

ROOT_DIR = '/opt/workspaces/'

DUMP_DOWNLOAD_DIR = os.path.join(ROOT_DIR, 'downloads/wikipedia_assistant/')

TMP_FILES_DIR = os.path.join(ROOT_DIR, 'tmp/')

SIMPLE_WIKI_BASE_URL = 'https://dumps.wikimedia.org/simplewiki/{YYYYMMDD}/'

# ETL-related table names
PROD_TABLE_NAMES = 'wikipedia_etl.production_table_names'
ETL_JOB_LOG = 'wikipedia_etl.etl_job_log'

# Newly published table names
PAGE_TIMESTAMPS = 'wikipedia.pagetimestamps'
PAGE_LINK_POSITIONS = 'wikipedia.pagelinkpositions'
MOST_OUTDATED_PAGE = 'wikipedia.most_outdated_page'

DDLS = {PAGE_TIMESTAMPS:        """
                                CREATE TABLE wikipedia.pagetimestamps(
                                    `page_id` int(8) unsigned NOT NULL,
                                    `page_namespace` int(11) NOT NULL,
                                    `page_title` varchar(255) NOT NULL,
                                    `last_modified` timestamp NOT NULL,
                                    PRIMARY KEY (`page_id`)
                                )
                                """,
        PAGE_LINK_POSITIONS:    """
                                CREATE TABLE wikipedia.pagelinkpositions(
                                    `pl_from` int(8) unsigned NOT NULL,
                                    `pl_from_namespace` int(11) NOT NULL,
                                    `pl_title` varchar(255) NOT NULL,
                                    `pl_namespace` int(11) NOT NULL,
                                    `pl_position` int unsigned NOT NULL,
                                    PRIMARY KEY (`pl_from`, `pl_title`)
                                )
                                """
        }
