#!/usr/bin/env -S python -u
import sys
from os.path import join, realpath, dirname
from datetime import datetime
from typing import List, Tuple
from requests import Session
from graphai_client.client_api.utils import login
from graphai_client.utils import (
    status_msg, execute_query, get_piper_connection, insert_keywords_and_concepts,
    update_data_into_table
)
from graphai_client.client_api.text import clean_text_translate_extract_keywords_and_concepts


def detect_concept_from_videos_on_rcp(
        videos_platform_and_id: List[Tuple[str, str]], analyze_subtitles=False, analyze_slides=True,
        graph_api_json=None, login_info=None, piper_mysql_json_file=None
):
    if login_info is None or 'token' not in login_info:
        login_info = login(graph_api_json)
    with Session() as session:
        with get_piper_connection(piper_mysql_json_file) as piper_connection:
            for platform, video_id in videos_platform_and_id:
                status_msg(
                    f'Processing video {video_id} on {platform}',
                    color='grey', sections=['GRAPHAI', 'CONCEPT DETECTION', 'PROCESSING']
                )
                if analyze_subtitles:
                    segments_info = execute_query(
                        piper_connection, f'''
                        SELECT segmentId, textEn 
                        FROM gen_video.Subtitles 
                        WHERE platform="{platform}" AND videoId="{video_id}";
                    ''')
                    status_msg(
                        f'Extracting concepts from {len(segments_info)} subtitles of video {video_id} on {platform}',
                        color='grey', sections=['GRAPHAI', 'SUBTITLES', 'CONCEPT DETECTION', 'PROCESSING']
                    )
                    num_segments_with_keywords = 0
                    num_concepts = 0
                    for segment_id, segment_text in segments_info:
                        if segment_id == 0 and \
                                segment_text.startswith('These subtitles have been generated automatically'):
                            segment_text = segment_text.replace('These subtitles have been generated automatically', '')
                        keywords_and_concepts = clean_text_translate_extract_keywords_and_concepts(
                            text_data=(segment_text,), login_info=login_info, session=session
                        )
                        if keywords_and_concepts:
                            num_segments_with_keywords += 1
                            num_concepts += len(keywords_and_concepts['concepts_and_scores'])
                        insert_keywords_and_concepts(
                            piper_connection, pk=(platform, video_id, segment_id),
                            keywords_and_concepts=keywords_and_concepts,
                            schemas_keyword='gen_video', table_keywords='Subtitles',
                            pk_columns_keywords=('platform', 'videoId', 'segmentId'), schemas_concepts='gen_video',
                            table_concepts='Subtitle_Concepts',
                            pk_columns_concepts=('platform', 'videoId', 'segmentId'),
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
                    now = str(datetime.now())
                    execute_query(
                        piper_connection,
                        f'''UPDATE `gen_video`.`Videos` 
                        SET `subtitlesConceptExtractionTime`="{now}"  
                        WHERE platform="{platform}" AND videoId="{video_id}"'''
                    )
                    piper_connection.commit()
                    status_msg(
                        f'Extracted {num_concepts} concepts from {num_segments_with_keywords}/{len(segments_info)} '
                        f'subtitles of video {video_id} on {platform}',
                        color='green', sections=['GRAPHAI', 'SUBTITLES', 'CONCEPT DETECTION', 'SUCCESS']
                    )
                if analyze_slides:
                    slides_info = execute_query(
                        piper_connection, f'''
                        SELECT slideNumber, textEn
                        FROM gen_video.Slides 
                        WHERE platform="{platform}" AND videoId="{video_id}";
                    ''')
                    status_msg(
                        f'Extracting concepts from {len(slides_info)} slides of video {video_id} on {platform}',
                        color='grey', sections=['GRAPHAI', 'SLIDES', 'CONCEPT DETECTION', 'PROCESSING']
                    )
                    num_slides_with_keywords = 0
                    num_concepts = 0
                    for slide_number, slide_text in slides_info:
                        keywords_and_concepts = clean_text_translate_extract_keywords_and_concepts(
                            text_data=(slide_text,), login_info=login_info, session=session
                        )
                        if keywords_and_concepts:
                            num_slides_with_keywords += 1
                            num_concepts += len(keywords_and_concepts['concepts_and_scores'])
                        insert_keywords_and_concepts(
                            piper_connection, pk=(platform, video_id, slide_number),
                            keywords_and_concepts=keywords_and_concepts,
                            schemas_keyword='gen_video', table_keywords='Slides',
                            pk_columns_keywords=('platform', 'videoId', 'slideNumber'), schemas_concepts='gen_video',
                            table_concepts='Slide_Concepts', pk_columns_concepts=('platform', 'videoId', 'slideNumber'),
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
                    slides_extraction_time = str(datetime.now())
                    update_data_into_table(
                        piper_connection, 'gen_video', "Videos",
                        columns=("slidesConceptExtractionTime",),
                        pk_columns=("platform", "videoId"),
                        data=[(slides_extraction_time, platform, video_id)]
                    )
                    piper_connection.commit()
                    status_msg(
                        f'Extracted {num_concepts} concepts from {num_slides_with_keywords}/{len(slides_info)} '
                        f'slides of video {video_id} on {platform}',
                        color='green', sections=['GRAPHAI', 'SLIDES', 'CONCEPT DETECTION', 'SUCCESS']
                    )
                status_msg(
                    f'The video {video_id} on {platform} has been processed',
                    color='green', sections=['GRAPHAI', 'CONCEPT DETECTION', 'SUCCESS']
                )


if __name__ == '__main__':
    executable_name = sys.argv.pop(0)
    analyze_subtitles_str = sys.argv.pop(0)
    analyze_subtitles = analyze_subtitles_str.lower() == 'true'
    analyze_slides_str = sys.argv.pop(0)
    analyze_slides = analyze_slides_str.lower() == 'true'
    if len(sys.argv) % 2 != 0:
        raise ValueError('You must give a platform and an id for each video you want to process')
    videos_platform_and_id = []
    video_platform = None
    for arg in sys.argv:
        if video_platform is None:
            video_platform = arg
        else:
            videos_platform_and_id.append((video_platform, arg))
            video_platform = None

    print(
        f'Detect concept for {len(videos_platform_and_id)} videos '
        f'{"" if analyze_subtitles else "not"} using subtitles and {"" if analyze_slides else "not"} using slides.'
    )

    config_dir = realpath(join(dirname(__file__), '..', 'config'))
    piper_mysql_json_file = join(config_dir, "piper_db.json")
    graphai_json_file = join(config_dir, "graphai-api.json")
    detect_concept_from_videos_on_rcp(
        videos_platform_and_id, analyze_subtitles=analyze_subtitles, analyze_slides=analyze_slides,
        piper_mysql_json_file=piper_mysql_json_file, graph_api_json=graphai_json_file
    )

    print('Done')
