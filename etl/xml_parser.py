import os
import gc
import time
import logging
import pandas as pd
from xml.etree.ElementTree import iterparse

from db.maria import MariaDBConnector
from etl.constants import DDLS, PAGE_TIMESTAMPS, PAGE_LINK_POSITIONS


class WikiXMLParser:

    def __init__(self, xml_path=None, con=None):
        if xml_path and not os.path.exists(xml_path):
            raise FileNotFoundError(xml_path)
        self.xml_path = xml_path

        if con:
            self.con = con
        else:
            self.con = MariaDBConnector()

        self.last_modified_table_name = PAGE_TIMESTAMPS
        self.page_links_table_name = PAGE_LINK_POSITIONS
        self.num_last_modified_records = 0
        self.num_page_links_records = 0

    def parse_and_load_all_pages_xml(self, file_path=None, batch_size=10000, break_after=None):
        if file_path is None:
            if not self.xml_path:
                raise ValueError('No file path is given')
            else:
                file_path = self.xml_path
        elif not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

        if not (1000 <= batch_size <= 100000):
            raise ValueError('Batch size must be between 1,000 & 100,000 to prevent too many or too little batches')
        if break_after and break_after <= 0:
            raise ValueError('break_after needs to be >= 1')

        # Delete existing tables first before starting load and check counts are 0 to be safe
        self._drop_and_create()
        rc1 = self.con.execute(f'SELECT COUNT(*) FROM {self.last_modified_table_name}', as_df=False)
        rc2 = self.con.execute(f'SELECT COUNT(*) FROM {self.page_links_table_name}', as_df=False)
        assert rc1[0][0] == 0 and rc2[0][0] == 0
        logging.info('Beginning parsing of XML file containing all wiki pages')

        cnt = 0; num_pages_parsed = 0; num_batches = 0
        found_first_page = False
        found_page_title = False; found_page_namespace = False
        found_page_id = False; found_timestamp = False
        last_modified = []; page_links = []
        record = {}

        context = iterparse(file_path, events=("start", "end"))
        context = iter(context)
        event, root = context.__next__()

        for event, elem in context:
            if event == 'start' and elem.tag.endswith('page'):
                if not found_first_page:
                    found_first_page = True

                if cnt > 0 and record['page_namespace'] in ('0', '1'):
                    # logging.info(f'Assembling record for page {num_pages_parsed + 1}')

                    if ':' in record['page_title']:
                        page_title = ':'.join(record['page_title'].split(':')[1:])
                    else:
                        page_title = record['page_title']
                    page_title = page_title.replace(' ', '_')

                    last_modified.append((
                        record['page_id'],
                        record['page_namespace'],
                        page_title,
                        record['timestamp']
                    ))

                    if 'text' in record:
                        links = ' '.join(record['text']).split('[[')[1:]
                        links = [l.split(']]')[0].split('|')[0] for l in links]
                        links = [l.replace(' ', '_') for l in links if ':' not in l]

                        page_links.append((
                            record['page_id'],
                            record['page_namespace'],
                            links
                        ))

                    record = {}
                    num_pages_parsed += 1

                cnt += 1
                found_page_title = False; found_page_namespace = False
                found_page_id = False; found_timestamp = False

                # Clear XML Tree root of all seen elements so far to prevent memory overload when parsing huge file
                root.clear()

                if num_pages_parsed > (num_batches * batch_size) and (num_pages_parsed % batch_size) == 0:
                    num_batches += 1
                    logging.info(f'Finished parsing batch #{num_batches}. Loading this batch to db')
                    self._write_parsed_data_to_db(last_modified=last_modified, page_links=page_links)
                    last_modified = []; page_links = []
                    gc.collect()

                if break_after and num_pages_parsed == break_after:
                    break

            elif found_first_page:
                if elem.tag.endswith('}title') and not found_page_title and elem.text:
                    record['page_title'] = elem.text
                    found_page_title = True

                elif elem.tag.endswith('}ns') and not found_page_namespace:
                    record['page_namespace'] = elem.text
                    found_page_namespace = True

                elif elem.tag.endswith('}id') and not found_page_id:
                    record['page_id'] = elem.text
                    found_page_id = True

                elif elem.tag.endswith('}timestamp') and not found_timestamp:
                    record['timestamp'] = elem.text
                    found_timestamp = True

                elif elem.tag.endswith('}text') and elem.text:
                    if 'text' not in record:
                        record['text'] = [elem.text]
                    else:
                        record['text'].append(elem.text)

            else:
                continue

        if num_pages_parsed > (num_batches * batch_size):
            num_batches += 1
            logging.info(f'Finished parsing batch no. {num_batches}. Loading this batch to db')
            self._write_parsed_data_to_db(last_modified=last_modified, page_links=page_links)

        logging.info('Finished parsing XML file. Summary:')
        logging.info(f'{num_pages_parsed} pages parsed for last_modfied & page_links data in {num_batches} batches')
        logging.info(f'Total {self.num_last_modified_records} records appended to {self.last_modified_table_name}')
        logging.info(f'Total {self.num_page_links_records} records appended to {self.page_links_table_name}')
        gc.collect()

    def _write_parsed_data_to_db(self, last_modified, page_links):
        df_last_modified = pd.DataFrame(
            last_modified,
            columns=['page_id', 'page_namespace', 'page_title', 'timestamp']
        )

        page_links_flat = []
        for page_id, page_ns, links in page_links:
            link_position = 1
            for link in links:
                page_links_flat.append((page_id, page_ns, link, '0', str(link_position)))
                link_position += 1

        df_page_links = pd.DataFrame(
            page_links_flat,
            columns=['pl_from', 'pl_from_namespace', 'pl_title', 'pl_namespace', 'pl_position']
        )

        logging.info('Parsed following last_modified and page_links data for this batch:')
        time.sleep(0.2)
        print('\n')
        print(df_last_modified)
        print('\n')
        print(df_page_links)

        self.con.upload_df(
            df=df_last_modified,
            table_name=self.last_modified_table_name,
            delimiter='|',
            name='last_modified'
        )
        self.num_last_modified_records += len(df_last_modified)

        self.con.upload_df(
            df=df_page_links,
            table_name=self.page_links_table_name,
            delimiter='|',
            name='page_links'
        )
        self.num_page_links_records += len(df_page_links)

    def _drop_and_create(self):
        for tbl in [self.last_modified_table_name, self.page_links_table_name]:
            drop_q = f'DROP TABLE IF EXISTS {tbl}'
            self.con.execute(query=drop_q)

            create_q = DDLS[tbl]
            self.con.execute(query=create_q)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    f_path = '/Users/varunprakasam/Downloads/simplewiki-20200620-pages-meta-current.xml'
    xml_parser = WikiXMLParser(xml_path=f_path)
    xml_parser.parse_and_load_all_pages_xml(batch_size=10000)
