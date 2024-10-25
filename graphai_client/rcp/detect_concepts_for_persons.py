#!/usr/bin/env -S python -u
import sys
from os.path import join, realpath, dirname
from typing import List, Union
from requests import Session
from graphai_client.client_api.utils import login
from graphai_client.utils import execute_query, get_piper_connection, insert_keywords_and_concepts
from graphai_client.client_api.text import clean_text_translate_extract_keywords_and_concepts


def detect_concept_from_persons_on_rcp(
        scipers: List[Union[int, str]], graph_api_json=None, login_info=None, piper_mysql_json_file=None, temp_tables=True
):
    if login_info is None or 'token' not in login_info:
        login_info = login(graph_api_json)
    schema = 'gen_people'
    person_simplified_info_isa_table = 'Person_Simplified_Info_ISA'
    person_to_page_mapping_table = 'Person_to_Page_Mapping'
    if temp_tables:
        person_simplified_info_isa_table += '_tmp'
        person_to_page_mapping_table += '_tmp'
    with Session() as session:
        with get_piper_connection(piper_mysql_json_file) as piper_connection:
            persons_info = execute_query(
                piper_connection,
                f"""SELECT 
                    p.SCIPER, 
                    p.BiographyEN
                FROM {schema}.{person_simplified_info_isa_table} as p
                WHERE p.SCIPER IN ({', '.join([str(s) for s in scipers])});"""
            )
            for sciper, biography in persons_info:
                if not biography:
                    continue
                keywords_and_concepts = clean_text_translate_extract_keywords_and_concepts(
                    text_data=(biography,), login_info=login_info, session=session, translate_to_en=True
                )
                insert_keywords_and_concepts(
                    piper_connection, pk=(sciper,), keywords_and_concepts=keywords_and_concepts,
                    schemas_keyword=schema, table_keywords=person_simplified_info_isa_table,
                    pk_columns_keywords=('SCIPER',), schemas_concepts=schema,
                    table_concepts=person_to_page_mapping_table, pk_columns_concepts=('SCIPER',),
                    key_concepts=(
                        'concept_id', 'concept_name', 'search_score', 'levenshtein_score',
                        'embedding_local_score', 'embedding_global_score', 'graph_score',
                        'ontology_local_score', 'ontology_global_score',
                        'embedding_keywords_score', 'graph_keywords_score', 'ontology_keywords_score',
                        'mixed_score'
                    ),
                    columns_concept=(
                        'PageId', 'PageTitle', 'SearchScore', 'LevenshteinScore',
                        'EmbeddingLocalScore', 'EmbeddingGlobalScore', 'GraphScore',
                        'OntologyLocalScore', 'OntologyGlobalScore',
                        'EmbeddingKeywordsScore', 'GraphKeywordsScore', 'OntologyKeywordsScore',
                        'MixedScore'
                    )
                )


if __name__ == '__main__':
    executable_name = sys.argv.pop(0)
    persons = sys.argv
    print(f'Detect concept for {len(persons)} persons.')

    config_dir = realpath(join(dirname(__file__), '..', 'config'))
    piper_mysql_json_file = join(config_dir, "piper_db.json")
    graphai_json_file = join(config_dir, "graphai-api.json")
    detect_concept_from_persons_on_rcp(
        persons, piper_mysql_json_file=piper_mysql_json_file, graph_api_json=graphai_json_file
    )

    print('Done')
