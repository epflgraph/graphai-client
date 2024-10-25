#!/usr/bin/env -S python -u
import sys
from os.path import join, dirname, realpath
from datetime import datetime, timedelta
from typing import List, Tuple
from graphai_client.client_api.utils import login
from graphai_client.utils import (
    status_msg, strfdelta, execute_query, get_piper_connection, insert_data_into_table, update_data_into_table
)
from graphai_client.client import (
    process_slides, translate_extracted_text, get_video_token_and_codec_types
)


def detect_slides_on_rcp(
        videos_platform_and_id: List[Tuple[str, str]], piper_mysql_json_file=None, graph_api_json=None,
        force=False, debug=False, force_download=False, sections=('GRAPHAI', 'SLIDES DETECTION')
):
    login_info = login(graph_api_json)
    with get_piper_connection(piper_mysql_json_file) as piper_connection:
        switch_video_channel_info = execute_query(
            piper_connection, 'SELECT DISTINCT SwitchVideoID, SwitchChannelID FROM gen_switchtube.Slide_Text;'
        )
        switch_ids_to_channel = {v: c for v, c in switch_video_channel_info}
        kaltura_to_switch_info = execute_query(
            piper_connection, '''
                SELECT 
                    k.kalturaVideoID,
                    m.switchtube_id
                FROM ca_kaltura.Videos AS k 
                LEFT JOIN man_kaltura.Mapping_Kaltura_Switchtube AS m ON m.kaltura_id=k.kalturaVideoId;
            ''')
        kaltura_to_switch_id = {i[0]: i[1] for i in kaltura_to_switch_info}
        for platform, video_id in videos_platform_and_id:
            switchtube_video_id = None
            switchtube_channel = None
            if platform == 'mediaspace':
                switchtube_video_id = kaltura_to_switch_id.get(video_id, None)
                switchtube_channel = switch_ids_to_channel.get(switchtube_video_id, None)
            elif platform == 'switchtube':
                switchtube_video_id = video_id
                switchtube_channel = switch_ids_to_channel.get(switchtube_video_id, None)
            if switchtube_video_id is not None and switchtube_channel is not None:
                # switch video already processed, we can skip slide extraction and OCR
                status_msg(
                    f'The video {platform} {video_id} has been found on switchtube as {switchtube_video_id}, '
                    'skipping slides detection', color='grey', sections=list(sections) + ['PROCESSING']
                )
                slides_detected_language, slides = get_slides_from_switchtube(
                    piper_connection, switchtube_channel, switchtube_video_id,
                    login_info, destination_languages=None, force=force, debug=debug
                )
            else:
                video_token, codec_types = get_video_token_and_codec_types(
                    platform, video_id, piper_connection, login_info,
                    force=force, force_download=force_download, debug=debug, sections=sections
                )
                if video_token is None or codec_types is None:
                    continue
                if 'video' in codec_types:
                    slides_detected_language, slides = process_slides(
                        video_token, login_info, force=force, slides_language=None,
                        destination_languages=None, debug=debug
                    )
                else:
                    status_msg(
                        f'no video track found for {video_id} on {platform}, slide detection is skipped.',
                        color='yellow', sections=list(sections) + ['WARNING']
                    )
                    slides_detected_language = 'NA'
                    slides = None
            if slides_detected_language is not None and slides is not None:
                slides_detection_time = str(datetime.now())
                register_slides(piper_connection, platform, video_id, slides, slides_detected_language)
                update_data_into_table(
                    piper_connection, 'gen_video', "Videos",
                    columns=("slidesDetectionTime", "slidesDetectedLanguage"),
                    pk_columns=("platform", "videoId"),
                    data=[(slides_detection_time, slides_detected_language, platform, video_id)]
                )
                piper_connection.commit()


def get_slides_from_switchtube(
        db, switch_channel, switch_video_id, login_info, destination_languages=None, force=False, debug=False
):
    # get slide text (in english in gen_switchtube.Slide_Text) from analyzed switchtube video
    slides_text = []
    num_slides_languages = {'en': 0, 'fr': 0}
    slides_video_info = execute_query(
        db, f'''
            SELECT 
                t.SlideID,
                SUBSTRING(t.SlideID,LENGTH(t.SwitchChannelID) + LENGTH(t.SwitchVideoID) + 3), 
                t.SlideText,
                SUM(IF(o.DetectedLanguage='fr', 1, 0)) AS Nfr,
                SUM(IF(o.DetectedLanguage='en', 1, 0)) AS Nen
            FROM gen_switchtube.Slide_Text AS t
            LEFT JOIN gen_switchtube.Slide_OCR AS o ON o.SlideID=t.SlideID AND Method='google (dtd)'
            WHERE SwitchChannelID='{switch_channel}' AND SwitchVideoID='{switch_video_id}' 
            GROUP BY SlideID
            ORDER BY SlideNumber;'''
    )
    for slide_id, timestamp, slide_text, n_fr, n_en in slides_video_info:
        slides_text.append({
            'en': slide_text,
            'timestamp': int(timestamp)
        })
        if n_fr > n_en:
            num_slides_languages['fr'] += 1
        elif n_en > n_fr:
            num_slides_languages['en'] += 1
    if num_slides_languages['fr'] > num_slides_languages['en']:
        slides_detected_language = 'fr'
    elif num_slides_languages['en'] > num_slides_languages['fr']:
        slides_detected_language = 'en'
    else:
        slides_detected_language = None
    if destination_languages:
        # translate slide text
        slides_text = translate_extracted_text(
            slides_text, login_info, source_language='en',
            destination_languages=destination_languages, force=force, debug=debug
        )
    slides = []
    for slide_idx, slide_text in enumerate(slides_text):
        slide = {
            'token': None,  # as we did not do slide detection we do not know the token
            'fingerprint': None,  # nor the fingerprint
            'timestamp': int(slide_text['timestamp']),
        }
        for k, v in slide_text.items():
            if k != 'timestamp':
                slide[k] = v
        slides.append(slide)
    return slides_detected_language, slides


def register_slides(
        db, platform, video_id, slides, slides_detected_language, sections=('VIDEO', 'PROCESSING', 'REGISTER SLIDES')
):
    if slides is None:
        return
    data_slides = []
    num_slide_orig = 0
    num_slide_french = 0
    num_slide_english = 0
    for slide_number, slide in enumerate(slides):
        slide_time = strfdelta(timedelta(seconds=slide['timestamp']), '{H:02}:{M:02}:{S:02}')
        slide_orig = slide.get(slides_detected_language, None)
        slide_french = slide.get('fr', None)
        slide_english = slide.get('en', None)
        data_slides.append(
            [
                platform, video_id, slide_number, slide['fingerprint'], slide['timestamp'], slide_time,
                slide_french, slide_english, slide_orig
            ]
        )
        if slide_orig:
            num_slide_orig += 1
        if slide_french:
            num_slide_french += 1
        if slide_english:
            num_slide_english += 1
    execute_query(
        db, f'DELETE FROM `gen_video`.`Slides` WHERE platform="{platform}" AND videoId="{video_id}"'
    )
    insert_data_into_table(
        db, 'gen_video', 'Slides',
        [
            'platform', 'videoId', 'slideNumber', 'fingerprint', 'timestamp', 'slideTime',
            'textFr', 'textEn', 'textOriginal'
        ],
        data_slides
    )
    msg = f'registered {num_slide_orig} texts from {len(data_slides)} sildes in {slides_detected_language}'
    translations = []
    if num_slide_french > 0 and slides_detected_language != 'fr':
        translations.append(
            f'French{" (" + str(num_slide_french) + ")" if num_slide_french != num_slide_orig else ""}'
        )
    if num_slide_english > 0 and slides_detected_language != 'en':
        translations.append(
            f'English{" (" + str(num_slide_english) + ")" if num_slide_english != num_slide_orig else ""}'
        )
    if translations:
        msg += f' and the translation in {" and ".join(translations)}'
    msg += f' for video {video_id} on {platform}'
    status_msg(msg, color='green', sections=list(sections) + ['SUCCESS'])


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
    print(f'Detect slides from {len(videos_platform_and_id)} videos.')

    config_dir = realpath(join(dirname(__file__), '..',  'config'))
    piper_mysql_json_file = join(config_dir, "piper_db.json")
    graphai_json_file = join(config_dir, "graphai-api.json")
    detect_slides_on_rcp(
        videos_platform_and_id, piper_mysql_json_file=piper_mysql_json_file, graph_api_json=graphai_json_file
    )

    print('Done')
