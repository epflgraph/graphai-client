from datetime import timedelta
from mysql.connector import connect as mysql_connect
from json import load as load_json
from os.path import dirname, join
from re import match
from graphai.utils import (
    status_msg, get_video_link_and_size, strfdelta, insert_line_into_table, convert_subtitle_into_segments,
    combine_language_segments, add_initial_disclaimer
)
from graphai.client import process_video, translate_extracted_text, translate_subtitles


def process_video_urls_on_rcp(
        videos_urls: list, analyze_audio=True, analyze_slides=True, destination_languages=('fr', 'en'), force=False,
        graph_ai_server='http://127.0.0.1:28800', debug=False, piper_mysql_json_file=None
):
    if piper_mysql_json_file is None:
        piper_mysql_json_file = join(dirname(__file__), 'config', 'piper_db.json')
    with open(piper_mysql_json_file) as fp:
        piper_con_info = load_json(fp)
    with mysql_connect(
        host=piper_con_info['host'], port=piper_con_info['port'], user=piper_con_info['user'],
        password=piper_con_info['password']
    ) as piper_connection:
        with piper_connection.cursor() as piper_cursor:
            kaltura_videos_id = []
            for video_url in videos_urls:
                video_url_redirect, octet_size = get_video_link_and_size(video_url)
                if video_url_redirect is None:
                    status_msg(
                        f'The video at {video_url} is not accessible',
                        color='yellow', sections=['VIDEO', 'WARNING']
                    )
                    continue
                kaltura_id = get_kaltura_id_from_url(video_url)
                if kaltura_id:
                    kaltura_videos_id.append(kaltura_id)
                    continue
                video_information = process_video(
                    video_url, analyze_audio=analyze_audio, analyze_slides=analyze_slides, force=force,
                    destination_languages=destination_languages, graph_ai_server=graph_ai_server, debug=debug
                )
                slides_detected_language = video_information['slides_language']
                audio_detected_language = video_information['audio_language']
                subtitles = video_information['subtitles']
                slides = video_information['slides']
                # update gen_kaltura with processed info
                if slides is not None:
                    piper_cursor.execute(
                        f'DELETE FROM `gen_video`.`Slides` WHERE VideoUrl="{video_url}"'
                    )
                    for slide_number, slide in enumerate(slides):
                        slide_time = strfdelta(timedelta(seconds=slide['timestamp']), '{H:02}:{M:02}:{S:02}')
                        insert_line_into_table(
                            piper_cursor, 'gen_kaltura', 'Slides',
                            (
                                'VideoUrl', 'slideNumber',
                                'timestamp', 'slideTime',
                                'textFr', 'textEn'
                            ),
                            (
                                video_url, slide_number,
                                slide['timestamp'], slide_time,
                                slide.get('fr', None), slide.get('en', None)
                            )
                        )
                if subtitles is not None:
                    piper_cursor.execute(
                        f'DELETE FROM `gen_video`.`Subtitles` WHERE VideoUrl="{video_url}"'
                    )
                    for idx, segment in enumerate(subtitles):
                        insert_line_into_table(
                            piper_cursor, 'gen_kaltura', 'Subtitles',
                            (
                                'VideoUrl', 'segmentId', 'startMilliseconds', 'endMilliseconds',
                                'startTime', 'endTime',
                                'textFr', 'textEn'
                            ),
                            (
                                video_url, idx, int(segment['start']*1000), int(segment['end']*1000),
                                strfdelta(timedelta(seconds=segment['start']), '{H:02}:{M:02}:{S:02}.{m:03}'),
                                strfdelta(timedelta(seconds=segment['end']), '{H:02}:{M:02}:{S:02}.{m:03}'),
                                segment.get('fr', None), segment.get('en', None)
                            )
                        )
                piper_cursor.execute(
                    f'DELETE FROM `gen_video`.`Videos` WHERE VideoUrl="{video_url}"'
                )
                insert_line_into_table(
                    piper_cursor, 'gen_kaltura', 'Videos',
                    (
                        'VideoUrl', 'kalturaUrl', 'thumbnailUrl', 'kalturaCreationTime', 'kalturaUpdateTime',
                        'title', 'description', 'kalturaOwner', 'kalturaCreator', 'tags', 'categories',
                        'kalturaEntitledEditors', 'msDuration', 'octetSize',
                        'slidesDetectedLanguage', 'audioDetectedLanguage', 'switchVideoId'
                    ),
                    (
                        video_url, octet_size, slides_detected_language, audio_detected_language
                    )
                )
                piper_connection.commit()
                status_msg(
                    f'The video {video_url} has been processed',
                    color='green', sections=['KALTURA', 'VIDEO', 'SUCCESS']
                )
    if len(kaltura_videos_id) > 0:
        process_videos_on_rcp(
            kaltura_videos_id, analyze_audio=analyze_audio, analyze_slides=analyze_slides,
            destination_languages=destination_languages, force=force,graph_ai_server=graph_ai_server, debug=debug,
            piper_mysql_json_file=piper_mysql_json_file
        )


def get_kaltura_id_from_url(url):
    m = match(r'(?:https?://)?api.cast.switch.ch/.*/entryId/(0_[\w]{8})', url)
    if m:
        return m.group(1)
    m = match(r'(?:https?://)?mediaspace.epfl.ch/media/(0_[\w]{8})', url)
    if m:
        return m.group(1)
    m = match(r'(?:https?://)?mediaspace.epfl.ch/media/[^/]*/(0_[\w]{8})', url)
    if m:
        return m.group(1)
    return None


def process_videos_on_rcp(
        kaltura_ids: list, analyze_audio=True, analyze_slides=True, destination_languages=('fr', 'en'), force=False,
        graph_ai_server='http://127.0.0.1:28800', debug=False, piper_mysql_json_file=None
):
    if piper_mysql_json_file is None:
        piper_mysql_json_file = join(dirname(__file__), 'config', 'piper_db.json')
    with open(piper_mysql_json_file) as fp:
        piper_con_info = load_json(fp)
    with mysql_connect(
        host=piper_con_info['host'], port=piper_con_info['port'], user=piper_con_info['user'],
        password=piper_con_info['password']
    ) as piper_connection:
        with piper_connection.cursor() as piper_cursor:
            piper_cursor.execute(
                'SELECT DISTINCT SwitchVideoID, SwitchChannelID FROM gen_switchtube.Slide_Text;'
            )
            switch_ids_to_channel_with_text_from_slides = {i[0]: i[1] for i in piper_cursor}
            piper_cursor.execute('''
                SELECT 
                    k.kalturaVideoID,
                    m.switchtube_id
                FROM ca_kaltura.Videos AS k 
                LEFT JOIN man_kaltura.Mapping_Kaltura_Switchtube AS m ON m.kaltura_id=k.kalturaVideoId;
            ''')
            kaltura_to_switch_id = {i[0]: i[1] for i in piper_cursor}
            for kaltura_video_id in kaltura_ids:
                status_msg(
                    f'Processing kaltura video {kaltura_video_id}',
                    color='grey', sections=['KALTURA', 'VIDEO', 'PROCESSING']
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
                video_details = list(piper_cursor)
                if len(video_details) == 0:
                    status_msg(
                        f'Skipping video {kaltura_video_id} as it does not exists in ca_kaltura.Videos.',
                        color='yellow', sections=['KALTURA', 'VIDEO', 'WARNING']
                    )
                    continue
                (
                    kaltura_url_api, thumbnail_url, kaltura_creation_time, kaltura_update_time, title,
                    description, kaltura_owner, kaltura_creator, tags, categories, kaltura_entitled_editor, ms_duration
                ) = video_details[0]
                kaltura_url, octet_size = get_video_link_and_size(kaltura_url_api)
                if kaltura_url is None:
                    status_msg(
                        f'The video at {kaltura_url_api} is not accessible',
                        color='yellow', sections=['KALTURA', 'VIDEO', 'WARNING']
                    )
                    continue
                switchtube_video_id = kaltura_to_switch_id.get(kaltura_video_id, None)
                if switchtube_video_id is not None \
                        and switchtube_video_id in switch_ids_to_channel_with_text_from_slides:
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
                        subtitles = get_subtitles_from_kaltura(
                            kaltura_video_id, piper_cursor=piper_cursor, force=force,
                            destination_languages=destination_languages, graph_ai_server=graph_ai_server, debug=debug
                        )
                        if subtitles:
                            video_information = process_video(
                                kaltura_url, analyze_audio=False, analyze_slides=False, force=force,
                                detect_audio_language=True, audio_language=None,
                                graph_ai_server=graph_ai_server, debug=debug
                            )
                            audio_detected_language = video_information['audio_language']
                        else:
                            video_information = process_video(
                                kaltura_url, analyze_audio=True, analyze_slides=False, force=force,
                                destination_languages=destination_languages, audio_language=None,
                                graph_ai_server=graph_ai_server, debug=debug
                            )
                            audio_detected_language = video_information['audio_language']
                            subtitles = video_information['subtitles']
                    else:
                        audio_detected_language = None
                        subtitles = None
                else:  # full processing of the video
                    subtitles = get_subtitles_from_kaltura(
                        kaltura_video_id, piper_cursor=piper_cursor, force=force,
                        destination_languages=destination_languages, graph_ai_server=graph_ai_server, debug=debug
                    )
                    if subtitles:
                        video_information = process_video(
                            kaltura_url, analyze_audio=False, analyze_slides=analyze_slides, force=force,
                            detect_audio_language=True, audio_language=None,
                            destination_languages=destination_languages, graph_ai_server=graph_ai_server, debug=debug
                        )
                    else:
                        video_information = process_video(
                            kaltura_url, analyze_audio=analyze_audio, analyze_slides=analyze_slides, force=force,
                            destination_languages=destination_languages, graph_ai_server=graph_ai_server, debug=debug
                        )
                        subtitles = video_information['subtitles']
                    slides_detected_language = video_information['slides_language']
                    audio_detected_language = video_information['audio_language']
                    slides = video_information['slides']
                # update gen_kaltura with processed info
                if slides is not None:
                    piper_cursor.execute(
                        f'DELETE FROM `gen_kaltura`.`Slides` WHERE kalturaVideoId="{kaltura_video_id}"'
                    )
                    for slide_number, slide in enumerate(slides):
                        slide_time = strfdelta(timedelta(seconds=slide['timestamp']), '{H:02}:{M:02}:{S:02}')
                        insert_line_into_table(
                            piper_cursor, 'gen_kaltura', 'Slides',
                            (
                                'kalturaVideoId', 'slideNumber',
                                'timestamp', 'slideTime',
                                'textFr', 'textEn'
                            ),
                            (
                                kaltura_video_id, slide_number,
                                slide['timestamp'], slide_time,
                                slide.get('fr', None),  slide.get('en', None)
                            )
                        )
                if subtitles is not None:
                    piper_cursor.execute(
                        f'DELETE FROM `gen_kaltura`.`Subtitles` WHERE kalturaVideoId="{kaltura_video_id}"'
                    )
                    for idx, segment in enumerate(subtitles):
                        insert_line_into_table(
                            piper_cursor, 'gen_kaltura', 'Subtitles',
                            (
                                'kalturaVideoId', 'segmentId', 'startMilliseconds', 'endMilliseconds',
                                'startTime', 'endTime',
                                'textFr', 'textEn'
                            ),
                            (
                                kaltura_video_id, idx, int(segment['start']*1000), int(segment['end']*1000),
                                strfdelta(timedelta(seconds=segment['start']), '{H:02}:{M:02}:{S:02}.{m:03}'),
                                strfdelta(timedelta(seconds=segment['end']), '{H:02}:{M:02}:{S:02}.{m:03}'),
                                segment.get('fr', None), segment.get('en', None)
                            )
                        )
                piper_cursor.execute(
                    f'DELETE FROM `gen_kaltura`.`Videos` WHERE kalturaVideoId="{kaltura_video_id}"'
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
                piper_connection.commit()
                status_msg(
                    f'The video {kaltura_video_id} has been processed',
                    color='green', sections=['KALTURA', 'VIDEO', 'SUCCESS']
                )


def get_subtitles_from_kaltura(
        kaltura_video_id, piper_cursor=None, piper_mysql_json_file=None, force=False,
        destination_languages=('en', 'fr'), graph_ai_server='http://127.0.0.1:28800', debug=False
):
    piper_connection = None
    if piper_cursor is None:
        if piper_mysql_json_file is None:
            piper_mysql_json_file = join(dirname(__file__), 'config', 'piper_db.json')
        with open(piper_mysql_json_file) as fp:
            piper_con_info = load_json(fp)
        piper_connection = mysql_connect(
                host=piper_con_info['host'], port=piper_con_info['port'], user=piper_con_info['user'],
                password=piper_con_info['password']
        )
        piper_cursor = piper_connection.cursor()
    subtitles_in_kaltura = {}
    piper_cursor.execute(f'''
        SELECT captionData, fileExt, language 
        FROM ca_kaltura.Captions WHERE kalturaVideoId='{kaltura_video_id}';
    ''')
    caption_info = list(piper_cursor)
    if len(caption_info) > 0:
        for caption_data, file_ext, language in caption_info:
            if 'french' in language.lower():
                lang = 'fr'
            elif 'english' in language.lower():
                lang = 'en'
            elif 'italian' in language.lower():
                lang = 'it'
            elif 'german' in language.lower():
                lang = 'de'
            else:
                print(f'Unknown caption language: {language}')
                continue
            subtitles_in_kaltura[lang] = convert_subtitle_into_segments(caption_data, file_ext=file_ext)
    if not subtitles_in_kaltura:
        return None
    subtitles = combine_language_segments(**subtitles_in_kaltura)
    if destination_languages:
        missing_destination_language = []
        for lang in destination_languages:
            if lang not in subtitles_in_kaltura:
                missing_destination_language.append(lang)
        if missing_destination_language:
            if 'en' in subtitles_in_kaltura:
                translate_from = 'en'
            elif 'fr' in subtitles_in_kaltura:
                translate_from = 'fr'
            elif 'de' in subtitles_in_kaltura:
                translate_from = 'de'
            elif 'it' in subtitles_in_kaltura:
                translate_from = 'it'
            else:
                translate_from = subtitles_in_kaltura.keys()[0]
            status_msg(
                f'translate transcription for {len(subtitles)} segments in {translate_from}',
                color='grey', sections=['GRAPHAI', 'TRANSLATE', 'PROCESSING']
            )
            subtitles = translate_subtitles(
                subtitles, force=force, source_language=translate_from,
                destination_languages=missing_destination_language,
                graph_ai_server=graph_ai_server, debug=debug
            )
            subtitles = add_initial_disclaimer(subtitles, restrict_lang=missing_destination_language)
    if piper_connection:
        piper_cursor.close()
        piper_connection.close()
    return subtitles
