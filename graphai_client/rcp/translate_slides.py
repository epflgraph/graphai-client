#!/usr/bin/env -S python -u
import sys
from os.path import join, dirname, realpath
from datetime import datetime
from typing import List, Tuple
from graphai_client.client_api.utils import login
from graphai_client.utils import status_msg, update_data_into_table, execute_query, get_piper_connection
from graphai_client.client import translate_extracted_text


def translate_slides_on_rcp(
        videos_platform_and_id: List[Tuple[str, str]], piper_mysql_json_file=None, graph_api_json=None,
        destination_languages=('fr', 'en'), force=False, debug=False,
        sections=('GRAPHAI', 'SLIDES TRANSLATION')
):
    login_info = login(graph_api_json)
    with get_piper_connection(piper_mysql_json_file) as piper_connection:
        for platform, video_id in videos_platform_and_id:
            slides_language_info = execute_query(
                piper_connection,
                f'''SELECT slidesDetectedLanguage FROM gen_video.Videos 
                WHERE platform="{platform}" AND videoId="{video_id}";'''
            )
            if len(slides_language_info) != 1 or len(slides_language_info[0]) != 1:
                status_msg(
                    f'The video {platform} {video_id} could not be found in gen_video.Videos',
                    color='red', sections=list(sections) + ['ERROR']
                )
                continue
            slides_language = slides_language_info[0][0]
            slides_info = execute_query(
                piper_connection,
                f'''SELECT slideNumber, textOriginal FROM gen_video.Slides 
                WHERE platform="{platform}" AND videoId="{video_id}" ORDER BY slideNumber;'''
            )
            slides_text = [{slides_language: text, 'slide_number': slide_num} for slide_num, text in slides_info]
            slides_text = translate_extracted_text(
                slides_text, login_info, force=force, source_language=slides_language,
                destination_languages=destination_languages, debug=debug
            )
            translation_data = [
                (slide.get('en', None), slide.get('fr', None), platform, video_id, slide['slide_number'])
                for slide in slides_text
            ]
            update_data_into_table(
                piper_connection, 'gen_video', 'Slides', columns=('textEn', 'textFr'),
                pk_columns=('platform', 'videoId', 'slideNumber'), data=translation_data
            )
            slides_translation_time = str(datetime.now())
            update_data_into_table(
                piper_connection, 'gen_video', "Videos",
                columns=("slidesTranslationTime",),
                pk_columns=("platform", "videoId"),
                data=[(slides_translation_time, platform, video_id)]
            )
            piper_connection.commit()


if __name__ == '__main__':
    executable_name = sys.argv.pop(0)
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
    print(f'Translate slides for {len(videos_platform_and_id)} videos.')

    config_dir = realpath(join(dirname(__file__), '..',  'config'))
    piper_mysql_json_file = join(config_dir, "piper_db.json")
    graphai_json_file = join(config_dir, "graphai-api.json")
    translate_slides_on_rcp(
        videos_platform_and_id, piper_mysql_json_file=piper_mysql_json_file, graph_api_json=graphai_json_file
    )

    print('Done')
