import json
import pandas as pd
from db.maria import MariaDBConnector
from etl.constants import MOST_OUTDATED_PAGE, PAGE_LINK_POSITIONS


def query_wiki_assistant(sql_query, as_dict=False, as_json=False):
    con = MariaDBConnector()
    df = con.execute(query=sql_query)

    if as_dict:
        json_val = df.to_json(orient='records')
        return json.loads(json_val)
    elif as_json:
        return df.to_json(orient='records')
    else:
        return df


def get_most_outdated_page(category, as_dict=False, as_json=False):
    con = MariaDBConnector()
    category_parsed = category.replace(' ', '_').lower()

    query = f"SELECT * FROM most_outdated_page " \
            f"WHERE LOWER(CONVERT(category_title, VARCHAR(255))) = '{category_parsed}'"
    df = con.execute(query=query)

    if len(df) > 0:
        df['most_outdated_page_modification_time'] = df.most_outdated_page_modification_time.astype(str)
        df['most_recently_modified_link_update_time'] = df.most_recently_modified_link_update_time.astype(str)

    if as_dict:
        json_val = df.to_json(orient='records')
        return json.loads(json_val)
    elif as_json:
        return df.to_json(orient='records')
    else:
        return df


def get_page_links_by_position_on_page(page_title, as_dict=False, as_json=False):
    con = MariaDBConnector()
    page_title_parsed = page_title.replace(' ', '_').lower()

    query1 = f"SELECT page_id FROM page WHERE LOWER(CONVERT(page_title, VARCHAR(255))) = '{page_title_parsed}'"
    rs1 = con.execute(query=query1, as_df=False)
    page_id = rs1[0][0] if rs1 else None

    if page_id:
        query2 = f"SELECT '{page_title}' AS page_title, " \
                 f"pl_title AS link_page_title, " \
                 f"pl_namespace AS link_page_namespace, " \
                 f"pl_position AS link_position_on_page " \
                 f"FROM {PAGE_LINK_POSITIONS} WHERE pl_from = {page_id}"
        df = con.execute(query=query2, as_df=True)

    else:
        df = pd.DataFrame([], columns=['page_title',
                                       'link_page_title',
                                       'link_page_namespace',
                                       'link_position_on_page'])

    if as_dict:
        json_val = df.to_json(orient='records')
        return json.loads(json_val)
    elif as_json:
        return df.to_json(orient='records')
    else:
        return df
