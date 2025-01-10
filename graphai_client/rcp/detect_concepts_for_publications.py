#!/usr/bin/env -S python -u
import sys
from os.path import join, realpath, dirname
from typing import List, Union
from requests import Session
from graphai_client.client_api.utils import login
from graphai_client.utils import status_msg, execute_query, get_piper_connection, insert_keywords_and_concepts
from graphai_client.client_api.text import clean_text_translate_extract_keywords_and_concepts


def detect_concept_from_publications_on_rcp(
        publication_ids: List[Union[int, str]], graph_api_json=None, login_info=None, piper_mysql_json_file=None,
        use_temp=True
):
    if login_info is None or 'token' not in login_info:
        login_info = login(graph_api_json)
    schema_publication = 'gen_infoscience'
    table_publication = 'Publications'
    schema_publication_subjects = 'ca_infoscience'
    table_publication_subjects = 'Publication_Subjects'
    schema_publication_concepts = 'gen_infoscience'
    table_publication_concepts = 'Publication_to_Page_Mapping'
    if use_temp:
        table_publication += '_tmp'
        # table_publication_subjects += '_tmp'
        table_publication_concepts += '_tmp'
    publications_without_keywords = []
    publications_without_concepts = []
    publications_ok = []
    num_concepts = 0
    with Session() as session:
        with get_piper_connection(piper_mysql_json_file) as piper_connection:
            publications_info = execute_query(
                piper_connection,
                f"""SELECT 
                    p.PublicationID,
                    p.Title,
                    p.Abstract,
                    GROUP_CONCAT(s.Topic SEPARATOR ';') AS Keywords
                FROM {schema_publication}.{table_publication} AS p
                LEFT JOIN {schema_publication_subjects}.{table_publication_subjects} AS s ON 
                    s.PublicationID = p.PublicationID
                WHERE p.PublicationID IN ({', '.join(['"' + str(p_id) + '"' for p_id in publication_ids])})
                GROUP BY p.PublicationID;"""
            )
            for pub_id, title, abstract, keywords_str in publications_info:
                keywords = None
                if keywords_str:
                    keywords = keywords_str.split(';')
                keywords_and_concepts = clean_text_translate_extract_keywords_and_concepts(
                    text_data=(title, abstract), additional_keywords=keywords, login_info=login_info, session=session,
                    translate_to_en=False
                )
                insert_keywords_and_concepts(
                    piper_connection, pk=(pub_id,), keywords_and_concepts=keywords_and_concepts,
                    schemas_keyword=schema_publication, table_keywords=table_publication,
                    pk_columns_keywords=('PublicationID',), schemas_concepts=schema_publication_concepts,
                    table_concepts=table_publication_concepts, pk_columns_concepts=('PublicationID',),
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
                if keywords_and_concepts is None or not keywords_and_concepts.get('keywords', None):
                    publications_without_keywords.append(str(pub_id))
                elif not keywords_and_concepts.get('concepts_and_scores', None):
                    publications_without_concepts.append(str(pub_id))
                else:
                    publications_ok.append(str(pub_id))
                    num_concepts += len(keywords_and_concepts['concepts_and_scores'])
    if publications_without_keywords:
        status_msg(
            f'No keyword extracted for {len(publications_without_keywords)}/{len(publication_ids)} publications: '
            f'{", ".join(publications_without_keywords)}',
            color='yellow', sections=['GRAPHAI', 'PUBLICATIONS', 'CONCEPT DETECTION', 'WARNING']
        )
    if publications_without_concepts:
        status_msg(
            f'No concept extracted for {len(publications_without_concepts)}/{len(publication_ids)} publications: '
            f'{", ".join(publications_without_concepts)}',
            color='yellow', sections=['GRAPHAI', 'PUBLICATIONS', 'CONCEPT DETECTION', 'WARNING']
        )
    if publications_ok:
        status_msg(
            f'Extracted {num_concepts} concepts from {len(publications_ok)}/{len(publication_ids)} publications.',
            color='green', sections=['GRAPHAI', 'PUBLICATIONS', 'CONCEPT DETECTION', 'SUCCESS']
        )


if __name__ == '__main__':
    executable_name = sys.argv.pop(0)
    publications = sys.argv
    print(f'Detect concept for {len(publications)} publications.')

    config_dir = realpath(join(dirname(__file__), '..', 'config'))
    piper_mysql_json_file = join(config_dir, "piper_db.json")
    graphai_json_file = join(config_dir, "graphai-api.json")
    detect_concept_from_publications_on_rcp(
        publications, piper_mysql_json_file=piper_mysql_json_file, graph_api_json=graphai_json_file
    )

    print('Done')
