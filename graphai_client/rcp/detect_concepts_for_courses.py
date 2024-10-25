#!/usr/bin/env -S python -u
import sys
from os.path import join, realpath, dirname
from typing import List
from requests import Session
from graphai_client.client_api.utils import login
from graphai_client.utils import execute_query, get_piper_connection, insert_keywords_and_concepts
from graphai_client.client_api.text import clean_text_translate_extract_keywords_and_concepts


def detect_concept_from_courses_on_rcp(
        course_codes: List[str], graph_api_json=None, login_info=None, piper_mysql_json_file=None
):
    if login_info is None or 'token' not in login_info:
        login_info = login(graph_api_json)
    with Session() as session:
        with get_piper_connection(piper_mysql_json_file) as piper_connection:
            courses_info = execute_query(
                piper_connection,
                f"""SELECT 
                    c.CourseCode,
                    c.AcademicYear,
                    c.CourseSummaryEN,
                    c.CourseContentsEN,
                    c.CourseKeywordsEN,
                    c.CourseRequiredCoursesEN,
                    c.CourseRecommendedCoursesEN,
                    c.CourseRequiredConceptsEN,
                    c.CourseBibliographyEN,
                    c.CourseSuggestedRefsEN,
                    c.CoursePrerequisiteForEN
                FROM gen_studyplan.Courses_tmp as c
                INNER JOIN (
                    SELECT
                        CourseCode,
                        MAX(AcademicYear) AS LatestAcademicYear
                    FROM gen_studyplan.Courses_tmp
                    GROUP BY CourseCode
                ) AS id_last ON id_last.CourseCode=c.CourseCode AND id_last.LatestAcademicYear=c.AcademicYear
                WHERE c.CourseCode IN ({', '.join([f'"{cc}"' for cc in course_codes])});
                """
            )
            for course_code, academic_year, *text_data in courses_info:
                keywords_and_concepts = clean_text_translate_extract_keywords_and_concepts(
                    text_data=text_data, login_info=login_info, session=session, translate_to_en=True
                )
                insert_keywords_and_concepts(
                    piper_connection, pk=(course_code, academic_year), keywords_and_concepts=keywords_and_concepts,
                    schemas_keyword='gen_studyplan', table_keywords='Courses_tmp',
                    pk_columns_keywords=('CourseCode', 'AcademicYear'), schemas_concepts='gen_studyplan',
                    table_concepts='Course_to_Page_Mapping_tmp', pk_columns_concepts=('CourseCode', 'AcademicYear'),
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
    courses = sys.argv
    print(f'Detect concept for {len(courses)} courses.')

    config_dir = realpath(join(dirname(__file__), '..', 'config'))
    piper_mysql_json_file = join(config_dir, "piper_db.json")
    graphai_json_file = join(config_dir, "graphai-api.json")
    detect_concept_from_courses_on_rcp(
        courses, piper_mysql_json_file=piper_mysql_json_file, graph_api_json=graphai_json_file
    )

    print('Done')
