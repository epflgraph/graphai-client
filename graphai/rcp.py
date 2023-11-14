from datetime import timedelta
from mysql.connector import connect as mysql_connect
from json import load as load_json
from os.path import dirname, join
from graphai.utils import status_msg, get_video_link_and_size, strfdelta, insert_line_into_table
from graphai.client import process_video, translate_extracted_text


def process_videos_on_rcp(
        kaltura_ids: list, analyze_audio=True, analyze_slides=True, destination_languages=('fr', 'en'), force=False,
        graph_ai_server='http://127.0.0.1:28800', debug=False, piper_mysql_json_file=None
):
    if piper_mysql_json_file is None:
        piper_mysql_json_file = join(dirname(__file__), 'config', 'piper_db.json')
    with open(piper_mysql_json_file) as fp:
        piper_con_info = load_json(fp)
    piper_connection = mysql_connect(
        host=piper_con_info['host'], port=piper_con_info['port'], user=piper_con_info['user'],
        password=piper_con_info['password']
    )
    piper_cursor = piper_connection.cursor()
    piper_cursor.execute(
        'SELECT DISTINCT SwitchVideoID, SwitchChannelID FROM gen_switchtube.Slide_Text;'
    )
    switch_ids_to_channel_with_text_from_slides = {i[0]: i[1] for i in list(piper_cursor)}
    piper_cursor.execute('''
        SELECT 
            k.kalturaVideoID,
            m.switchtube_id
        FROM ca_kaltura.Videos AS k 
        LEFT JOIN man_kaltura.Mapping_Kaltura_Switchtube AS m ON m.kaltura_id=k.kalturaVideoId;
    ''')
    kaltura_to_switch_id = {i[0]: i[1] for i in list(piper_cursor)}
    for kaltura_video_id in kaltura_ids:
        status_msg(
            f'Processing kaltura video {kaltura_video_id}', color='grey', sections=['KALTURA', 'VIDEO', 'PROCESSING']
        )
        piper_cursor.execute(f'''
            SELECT 
                k.downloadUrl AS kaltura_url,
                k.thumbnailUrl,
                k.createdAt AS kalturaCreationTime,
                k.UpdatedAt AS kalturaUpdateTime,
                k.name as title,
                k.description,
                k.userId AS kalturaOwner,
                k.creatorId AS kalturaCreator,
                k.tags,
                k.categories,
                k.entitledUsersEdit AS kalturaEntitledEditors,
                k.msDuration
            FROM ca_kaltura.Videos AS k
            WHERE k.kalturaVideoID="{kaltura_video_id}";
        ''')
        (
            kaltura_url_api, thumbnail_url, kaltura_creation_time, kaltura_update_time, title,
            description, kaltura_owner, kaltura_creator, tags, categories, kaltura_entitled_editor, ms_duration
        ) = next(piper_cursor)
        kaltura_url, octet_size = get_video_link_and_size(kaltura_url_api)
        if kaltura_url is None:
            status_msg(
                f'The video at {kaltura_url_api} is not accessible',
                color='yellow', sections=['KALTURA', 'VIDEO', 'WARNING']
            )
            continue
        switchtube_video_id = kaltura_to_switch_id.get(kaltura_video_id, None)
        if switchtube_video_id is not None and switchtube_video_id in switch_ids_to_channel_with_text_from_slides:
            # switch video already processed, we can skip slide extraction and OCR
            status_msg(
                f'The video {kaltura_video_id} has been found on switchtube as {switchtube_video_id}, '
                'skipping slides detection', color='grey', sections=['KALTURA', 'VIDEO', 'PROCESSING']
            )
            switch_channel = switch_ids_to_channel_with_text_from_slides[switchtube_video_id]
            # get slide text (in english in gen_switchtube.Slide_Text) from analyzed switchtube video
            slides_text = []
            piper_cursor.execute(f'''
                SELECT 
                    SlideID,
                    SUBSTRING(SlideID,LENGTH(SwitchChannelID) + LENGTH(SwitchVideoID) + 3), 
                    SlideText 
                FROM gen_switchtube.Slide_Text 
                WHERE SwitchChannelID='{switch_channel}' AND SwitchVideoID='{switchtube_video_id}' 
                ORDER BY SlideNumber;
            ''')
            for slide_id, timestamp, slide_text in piper_cursor:
                slides_text.append({
                    'en': slide_text,
                    'timestamp': int(timestamp)
                })
            # translate slide text
            status_msg(
                f'translate text from {len(slides_text)} slides in en',
                color='grey', sections=['GRAPHAI', 'TRANSLATE', 'PROCESSING']
            )
            slides_detected_language = None
            slides_text = translate_extracted_text(
                slides_text, source_language='en',
                destination_languages=destination_languages, force=force,
                graph_ai_server=graph_ai_server, debug=debug
            )
            slides = []
            for slide_idx, slide_text in enumerate(slides_text):
                slide = {
                    'token': None,  # as we did not do slide detection we do not know the token
                    'timestamp': int(slide_text['timestamp']),
                }
                for k, v in slide_text.items():
                    if k != 'timestamp':
                        slide[k] = v
                slides.append(slide)
            if analyze_audio:
                video_information = process_video(
                    kaltura_url, analyze_audio=True, analyze_slides=False, force=force,
                    graph_ai_server=graph_ai_server, debug=debug
                )
                audio_detected_language = video_information['audio_language']
                subtitles = video_information['subtitles']
            else:
                audio_detected_language = None
                subtitles = None
        else:  # full processing of the video
            video_information = process_video(
                kaltura_url, analyze_audio=analyze_audio, analyze_slides=analyze_slides, force=force,
                graph_ai_server=graph_ai_server, debug=debug
            )
            slides_detected_language = video_information['slides_language']
            audio_detected_language = video_information['audio_language']
            subtitles = video_information['subtitles']
            slides = video_information['slides']
        # update gen_kaltura with processed info
        if slides is not None:
            for slide_number, slide in enumerate(slides):
                insert_line_into_table(
                    piper_cursor, 'gen_kaltura', 'Slides',
                    (
                        'kalturaVideoId', 'slideNumber',
                        'timestamp', 'slideTime',
                        'textFr','textEn'
                    ),
                    (
                        kaltura_video_id, slide_number,
                        slide['timestamp'], strfdelta(timedelta(seconds=slide['timestamp']), '{H:02}:{M:02}:{S:02}'),
                        slide.get('fr', None),  slide.get('en', None)
                    )
                )
        if subtitles is not None:
            for idx, segment in enumerate(subtitles):
                insert_line_into_table(
                    piper_cursor, 'gen_kaltura', 'Subtitles',
                    (
                        'kalturaVideoId', 'segmentId', 'startMilliseconds', 'endMilliseconds',
                        'startTime', 'endTime',
                        'textFr', 'textEn'
                    ),
                    (
                        kaltura_video_id, idx, segment['start'], segment['end'],
                        strfdelta(timedelta(seconds=segment['start']), '{H:02}:{M:02}:{S:02}.{m:03}'),
                        strfdelta(timedelta(seconds=segment['end']), '{H:02}:{M:02}:{S:02}.{m:03}'),
                        segment.get('fr', None), segment.get('en', None)
                    )
                )
        insert_line_into_table(
            piper_cursor, 'gen_kaltura', 'Videos',
            (
                'kalturaVideoId', 'kalturaUrl', 'thumbnailUrl', 'kalturaCreationTime', 'kalturaUpdateTime',
                'title', 'description', 'kalturaOwner', 'kalturaCreator', 'tags', 'categories',
                'kalturaEntitledEditors', 'msDuration', 'octetSize',
                'slidesDetectedLanguage', 'audioDetectedLanguage', 'switchVideoId'
            ),
            (
                kaltura_video_id, kaltura_url, thumbnail_url, kaltura_creation_time, kaltura_update_time,
                title, description, kaltura_owner, kaltura_creator, tags, categories,
                kaltura_entitled_editor, ms_duration, octet_size,
                slides_detected_language, audio_detected_language, switchtube_video_id
            )
        )
        status_msg(
            f'The video {kaltura_video_id} has been processed', color='green', sections=['KALTURA', 'VIDEO', 'SUCCESS']
        )
