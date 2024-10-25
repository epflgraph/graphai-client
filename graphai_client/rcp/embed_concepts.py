#!/usr/bin/env -S python -u
import sys
from json import dumps
from os.path import join, realpath, dirname
from typing import List, Union
from graphai_client.client_api.utils import login
from graphai_client.utils import status_msg, execute_query, get_piper_connection, update_data_into_table
from graphai_client.client_api.embedding import embed_text


def compute_embeddings_of_concepts_on_rcp(
        page_ids: List[Union[int, str]], graph_api_json=None, login_info=None, piper_mysql_json_file=None,
        batch_size=10000, temp_tables=True, max_text_length=400
):
    if login_info is None or 'token' not in login_info:
        login_info = login(graph_api_json)
    num_pages = len(page_ids)
    if num_pages > batch_size:
        for idx in range(0, num_pages, batch_size):
            compute_embeddings_of_concepts_on_rcp(
                page_ids[idx:(idx+batch_size)], graph_api_json=graph_api_json, login_info=login_info,
                piper_mysql_json_file=piper_mysql_json_file, batch_size=batch_size
            )
        return
    schema = 'gen_wikipedia'
    page_neighbours_table = 'Pages_Neighbours'
    page_content_table = 'Page_Content_Full'
    if temp_tables:
        page_neighbours_table += '_tmp'
        page_content_table += '_tmp'
    with get_piper_connection(piper_mysql_json_file) as piper_connection:
        concepts_info = execute_query(
            piper_connection,
            f"""SELECT 
                p.PageID, 
                pc.OpeningText
            FROM {schema}.{page_neighbours_table} as p
            INNER JOIN {schema}.{page_content_table} AS pc USING (PageID)
            WHERE p.PageID IN ({', '.join([str(p_id) for p_id in page_ids])});"""
        )
        text_to_embed = []
        page_id_of_embedded_text = []
        for page_id, opening_text in concepts_info:
            page_id_of_embedded_text.append(page_id)
            text_to_embed.append(opening_text)
        status_msg(
            f'embedding {len(page_id_of_embedded_text)} concepts...',
            color='gray', sections=('GRAPHAI', 'EMBED CONCEPT', 'SUCCESS')
        )
        embedded_texts = embed_text(text_to_embed, login_info=login_info, max_text_length=max_text_length)
        assert len(embedded_texts) == len(page_id_of_embedded_text)
        data_embeddings = [
            (
                dumps([round(e, 10) for e in embedding]) if embedding else None,
                page_id
            )
            for page_id, embedding in zip(page_id_of_embedded_text, embedded_texts)
        ]
        update_data_into_table(
            piper_connection, schema=schema, table_name=page_neighbours_table,
            columns=['Embedding'], pk_columns=['PageID'], data=data_embeddings
        )
        piper_connection.commit()
    status_msg(
        f'Embeddings have been computed for {num_pages} concepts in the ontology neighbourhood', color='green',
        sections=['GRAPHAI', 'EMBED CONCEPT', 'SUCCESS']
    )


if __name__ == '__main__':
    executable_name = sys.argv.pop(0)
    temp_tables_str = sys.argv.pop(0)
    temp_tables = temp_tables_str.lower() == 'true'
    concepts = sys.argv
    print(f'Embed {len(concepts)} concepts.')

    config_dir = realpath(join(dirname(__file__), '..', 'config'))
    piper_mysql_json_file = join(config_dir, "piper_db.json")
    graphai_json_file = join(config_dir, "graphai-api.json")
    compute_embeddings_of_concepts_on_rcp(
        concepts, piper_mysql_json_file=piper_mysql_json_file, graph_api_json=graphai_json_file, temp_tables=temp_tables
    )

    print('Done')
