from datetime import timedelta, datetime
from mysql.connector import connect as mysql_connect
from json import load as load_json
from os.path import dirname, join
from re import match, fullmatch
from requests import Session
from graphai.utils import (
    status_msg, get_video_link_and_size, strfdelta, insert_line_into_table_with_types, convert_subtitle_into_segments,
    combine_language_segments, add_initial_disclaimer, default_disclaimer, default_missing_transcript,
    insert_data_into_table_with_type, execute_query, prepare_value_for_mysql
)
from graphai.client import process_video, translate_extracted_text, translate_subtitles
from graphai.client_api.text import extract_concepts_from_text
from graphai.client_api.translation import translate_text

language_to_short = {
    'french': 'fr',
    'english': 'en',
    'italian': 'it',
    'german': 'de'
}
short_to_language = {v: k for k, v in language_to_short.items()}


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
                        insert_line_into_table_with_types(
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
                            ),
                            (
                                'str', 'int',
                                'int', 'str',
                                'str', 'str'
                            )
                        )
                if subtitles is not None:
                    piper_cursor.execute(
                        f'DELETE FROM `gen_video`.`Subtitles` WHERE VideoUrl="{video_url}"'
                    )
                    for idx, segment in enumerate(subtitles):
                        insert_line_into_table_with_types(
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
                            ),
                            (
                                'str', 'int', 'int', 'int',
                                'str', 'str',
                                'str', 'str'
                            )
                        )
                piper_cursor.execute(
                    f'DELETE FROM `gen_video`.`Videos` WHERE VideoUrl="{video_url}"'
                )
                insert_line_into_table_with_types(
                    piper_cursor, 'gen_kaltura', 'Videos',
                    (
                        'VideoUrl', 'kalturaUrl', 'thumbnailUrl', 'kalturaCreationTime', 'kalturaUpdateTime',
                        'title', 'description', 'kalturaOwner', 'kalturaCreator', 'tags', 'categories',
                        'kalturaEntitledEditors', 'msDuration', 'octetSize',
                        'slidesDetectedLanguage', 'audioDetectedLanguage', 'switchVideoId'
                    ),
                    (
                        video_url, octet_size, slides_detected_language, audio_detected_language
                    ),
                    (
                        'str', 'str', 'str', 'str', 'str',
                        'str', 'str', 'str', 'str', 'str', 'str',
                        'str', 'int', 'int',
                        'str', 'str', 'str'
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
                if not kaltura_url_api:
                    status_msg(
                        f'Skipping video {kaltura_video_id} which has no download link',
                        color='yellow', sections=['KALTURA', 'VIDEO', 'WARNING']
                    )
                    continue
                kaltura_url, octet_size = get_video_link_and_size(kaltura_url_api)
                if kaltura_url is None:
                    status_msg(
                        f'The video at {kaltura_url_api} is not accessible',
                        color='yellow', sections=['KALTURA', 'VIDEO', 'WARNING']
                    )
                    continue
                audio_transcription_time = None
                slides_detection_time = None
                switchtube_video_id = kaltura_to_switch_id.get(kaltura_video_id, None)
                if switchtube_video_id is not None \
                        and switchtube_video_id in switch_ids_to_channel_with_text_from_slides:
                    if analyze_slides:
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
                        slides_detection_time = str(datetime.now())
                    else:
                        slides = None
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
                            audio_transcription_time = str(datetime.now())
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
                        audio_transcription_time = str(datetime.now())
                        if analyze_slides:
                            slides_detection_time = str(datetime.now())
                    else:
                        video_information = process_video(
                            kaltura_url, analyze_audio=analyze_audio, analyze_slides=analyze_slides, force=force,
                            destination_languages=destination_languages, graph_ai_server=graph_ai_server, debug=debug
                        )
                        subtitles = video_information['subtitles']
                        if analyze_audio:
                            audio_transcription_time = str(datetime.now())
                        if analyze_slides:
                            slides_detection_time = str(datetime.now())
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
                        insert_line_into_table_with_types(
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
                            ),
                            (
                                'str', 'int',
                                'int', 'str',
                                'str', 'str'
                            )
                        )
                if subtitles is not None:
                    piper_cursor.execute(
                        f'DELETE FROM `gen_kaltura`.`Subtitles` WHERE kalturaVideoId="{kaltura_video_id}"'
                    )
                    for idx, segment in enumerate(subtitles):
                        insert_line_into_table_with_types(
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
                            ),
                            (
                                'str', 'int', 'int', 'int',
                                'str', 'str',
                                'str', 'str'
                            )
                        )
                piper_cursor.execute(
                    f'DELETE FROM `gen_kaltura`.`Videos` WHERE kalturaVideoId="{kaltura_video_id}"'
                )
                insert_line_into_table_with_types(
                    piper_cursor, 'gen_kaltura', 'Videos',
                    (
                        'kalturaVideoId', 'kalturaUrl', 'thumbnailUrl', 'kalturaCreationTime', 'kalturaUpdateTime',
                        'title', 'description', 'kalturaOwner', 'kalturaCreator', 'tags', 'categories',
                        'kalturaEntitledEditors', 'msDuration', 'octetSize',
                        'slidesDetectedLanguage', 'audioDetectedLanguage', 'switchVideoId',
                        'slidesDetectionTime', 'audioTranscriptionTime'
                    ),
                    (
                        kaltura_video_id, kaltura_url, thumbnail_url, kaltura_creation_time, kaltura_update_time,
                        title, description, kaltura_owner, kaltura_creator, tags, categories,
                        kaltura_entitled_editor, ms_duration, octet_size,
                        slides_detected_language, audio_detected_language, switchtube_video_id,
                        slides_detection_time, audio_transcription_time
                    ),
                    (
                        'str', 'str', 'str', 'str', 'str',
                        'str', 'str', 'str', 'str', 'str', 'str',
                        'str', 'int', 'int',
                        'str', 'str', 'str',
                        'str', 'str'
                    )
                )
                piper_connection.commit()
                status_msg(
                    f'The video {kaltura_video_id} has been processed',
                    color='green', sections=['KALTURA', 'VIDEO', 'SUCCESS']
                )


def get_piper_cursor(piper_connection=None, piper_mysql_json_file=None, piper_cursor=None, ):
    if piper_connection is None and piper_cursor is None:
        if piper_mysql_json_file is None:
            piper_mysql_json_file = join(dirname(__file__), 'config', 'piper_db.json')
        with open(piper_mysql_json_file) as fp:
            piper_con_info = load_json(fp)
        piper_connection = mysql_connect(
            host=piper_con_info['host'], port=piper_con_info['port'], user=piper_con_info['user'],
            password=piper_con_info['password']
        )
    if piper_cursor is None:
        piper_cursor = piper_connection.cursor()
    return piper_connection, piper_cursor


def get_subtitles_from_kaltura(
        kaltura_video_id, piper_cursor=None, piper_mysql_json_file=None, force=False,
        destination_languages=('en', 'fr'), graph_ai_server='http://127.0.0.1:28800',
        ignore_autogenerated=True, debug=False
):
    close_cursor = piper_cursor is None
    piper_connection, piper_cursor = get_piper_cursor(
        piper_mysql_json_file=piper_mysql_json_file, piper_cursor=piper_cursor
    )
    subtitle_query = f'''
        SELECT captionData, fileExt, language 
        FROM ca_kaltura.Captions WHERE kalturaVideoId='{kaltura_video_id}'
    '''
    if ignore_autogenerated:
        piper_cursor.execute(f'''
            SELECT partnerData FROM ca_kaltura.Videos WHERE kalturaVideoId='{kaltura_video_id}';
        ''')
        partner_data_info = list(piper_cursor)
        languages_with_auto_captions = []
        if len(partner_data_info) > 0:
            if partner_data_info[0][0]:
                partner_data = partner_data_info[0][0].split(',')
                for data in partner_data:
                    matched = fullmatch(r'sub_([a-z]+)_auto', data)
                    if matched:
                        language_short = matched.group(1).lower()
                        languages_with_auto_captions.append('"' + short_to_language[language_short] + '"')
        if languages_with_auto_captions:
            subtitle_query += f' AND language NOT IN ({", ".join(languages_with_auto_captions)})'
    subtitles_in_kaltura = {}
    piper_cursor.execute(subtitle_query)
    caption_info = list(piper_cursor)
    if len(caption_info) > 0:
        for caption_data, file_ext, language in caption_info:
            if caption_data is None:
                continue
            if language.lower() in language_to_short:
                lang = language_to_short[language.lower()]
            else:
                status_msg(
                    f'Unknown caption language: {language}',
                    sections=['GRAPHAI', 'GET SUBTITLES', 'WARNING'], color='yellow'
                )
                continue
            segments = convert_subtitle_into_segments(caption_data, file_ext=file_ext)
            if segments and len(segments) == 1 and segments[0]['text'] == default_missing_transcript.get(lang, None):
                continue
            subtitles_in_kaltura[lang] = segments
    if not subtitles_in_kaltura:
        return None
    if ignore_autogenerated:
        languages_to_ignore = []
        for lang, subtitles in subtitles_in_kaltura.items():
            if subtitles[0]:
                if subtitles[0]['text'].split('\n')[0] == default_disclaimer[lang]:
                    msg = f'Found automatic captions not tagged by "sub_{lang}_auto" in partnerData ' + \
                          f'for video "{kaltura_video_id}"'
                    status_msg(msg, sections=['GRAPHAI', 'GET SUBTITLES', 'WARNING'], color='yellow')
                    languages_to_ignore.append(lang)
        for lang in languages_to_ignore:
            del subtitles_in_kaltura[lang]
    subtitles = combine_language_segments(**subtitles_in_kaltura, precision_s=2)
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
    if close_cursor:
        piper_cursor.close()
        piper_connection.close()
    return subtitles


def detect_concept_on_rcp(
        kaltura_ids: list, analyze_subtitles=False, analyze_slides=True, graph_ai_server='http://127.0.0.1:28800',
        piper_mysql_json_file=None, piper_connection=None
):
    close_connection = piper_connection is None
    piper_connection, piper_cursor = get_piper_cursor(
        piper_connection=piper_connection, piper_mysql_json_file=piper_mysql_json_file
    )
    session = Session()
    for video_id in kaltura_ids:
        status_msg(
            f'Processing kaltura video {video_id}',
            color='grey', sections=['KALTURA', 'CONCEPT DETECTION', 'PROCESSING']
        )
        if analyze_subtitles:
            piper_cursor.execute(f'''
                SELECT 
                    segmentId,
                    textEn
                FROM gen_kaltura.Subtitles WHERE kalturaVideoId="{video_id}";
            ''')
            segments_info = list(piper_cursor)
            status_msg(
                f'Extracting concepts from {len(segments_info)} subtitles of video {video_id}',
                color='grey', sections=['KALTURA', 'CONCEPT DETECTION', 'SUBTITLES', 'PROCESSING']
            )
            piper_cursor.execute(
                f'DELETE FROM `gen_kaltura`.`Subtitle_Concepts` WHERE kalturaVideoId="{video_id}";'
            )
            segments_processed = 0
            concepts_segments_data = []
            for segment_id, segment_text in segments_info:
                if not segment_text:
                    continue
                segment_scores = extract_concepts_from_text(
                    segment_text, graph_ai_server=graph_ai_server,
                    sections=['KALTURA', 'CONCEPT DETECTION', 'SUBTITLES'], session=session
                )
                segments_processed += 1
                if segment_scores is None:
                    continue
                concepts_segments_data.extend([
                    (
                        video_id, segment_id, scores['PageID'], scores['PageTitle'], scores['SearchScore'],
                        scores['LevenshteinScore'], scores['GraphScore'], scores['OntologyLocalScore'],
                        scores['OntologyGlobalScore'], scores['KeywordsScore'], scores['MixedScore']
                    ) for scores in segment_scores
                ])
            insert_data_into_table_with_type(
                piper_cursor, schema='gen_kaltura', table_name='Subtitle_Concepts',
                columns=(
                    'kalturaVideoId', 'segmentId', 'PageId', 'PageTitle', 'SearchScore',
                    'LevenshteinScore', 'GraphScore', 'OntologyLocalScore',
                    'OntologyGlobalScore', 'KeywordsScore', 'MixedScore'
                ),
                data=concepts_segments_data,
                types=(
                    'str', 'int', 'int', 'str', 'float',
                    'float', 'float', 'float',
                    'float', 'float', 'float'
                )
            )
            now = str(datetime.now())
            if segments_processed > 0:
                piper_cursor.execute(
                    f'''UPDATE `gen_kaltura`.`Videos` 
                    SET `subtitlesConceptExtractionTime`="{now}" 
                    WHERE kalturaVideoId="{video_id}"'''
                )
                piper_connection.commit()
                status_msg(
                    f'Concepts have been extracted from {segments_processed}/{len(segments_info)} subtitles of {video_id}',
                    color='green', sections=['KALTURA', 'CONCEPT DETECTION', 'SUBTITLES', 'SUCCESS']
                )
            else:
                status_msg(
                    f'No usuable sutitles found for video {video_id}',
                    color='yellow', sections=['KALTURA', 'CONCEPT DETECTION', 'SUBTITLES', 'WARNING']
                )
        if analyze_slides:
            piper_cursor.execute(f'''
                SELECT 
                    slideNumber,
                    textEn
                FROM gen_kaltura.Slides WHERE kalturaVideoId="{video_id}";
            ''')
            slides_info = list(piper_cursor)
            status_msg(
                f'Extracting concepts from {len(slides_info)} slides of video {video_id}',
                color='grey', sections=['KALTURA', 'CONCEPT DETECTION', 'SLIDES', 'PROCESSING']
            )
            piper_cursor.execute(
                f'DELETE FROM `gen_kaltura`.`Slide_Concepts` WHERE kalturaVideoId="{video_id}";'
            )
            slides_processed = 0
            concepts_slides_data = []
            for slide_number, slide_text in slides_info:
                if not slide_text:
                    continue
                slide_scores = extract_concepts_from_text(
                    slide_text, graph_ai_server=graph_ai_server, sections=['KALTURA', 'CONCEPT DETECTION', 'SLIDES']
                )
                slides_processed += 1
                if slide_scores is None:
                    continue
                concepts_slides_data.extend([
                    (
                        video_id, slide_number, scores['PageID'], scores['PageTitle'], scores['SearchScore'],
                        scores['LevenshteinScore'], scores['GraphScore'], scores['OntologyLocalScore'],
                        scores['OntologyGlobalScore'], scores['KeywordsScore'], scores['MixedScore']
                    ) for scores in slide_scores
                ])
            insert_data_into_table_with_type(
                piper_cursor, schema='gen_kaltura', table_name='Slide_Concepts',
                columns=(
                    'kalturaVideoId', 'slideNumber', 'PageId', 'PageTitle', 'SearchScore',
                    'LevenshteinScore', 'GraphScore', 'OntologyLocalScore',
                    'OntologyGlobalScore', 'KeywordsScore', 'MixedScore'
                ),
                data=concepts_slides_data,
                types=(
                    'str', 'int', 'int', 'str', 'float',
                    'float', 'float', 'float',
                    'float', 'float', 'float'
                )
            )
            if slides_processed > 0:
                now = str(datetime.now())
                piper_cursor.execute(
                    f'''UPDATE `gen_kaltura`.`Videos` 
                    SET `slidesConceptExtractionTime`="{now}" 
                    WHERE kalturaVideoId="{video_id}"'''
                )
                piper_connection.commit()
                status_msg(
                    f'Concepts have been extracted from {slides_processed}/{len(slides_info)} slides of {video_id}',
                    color='green', sections=['KALTURA', 'CONCEPT DETECTION', 'SLIDES', 'SUCCESS']
                )
            else:
                status_msg(
                    f'No usable slides found for video {video_id}',
                    color='yellow', sections=['KALTURA', 'CONCEPT DETECTION', 'SLIDES', 'WARNING']
                )
        status_msg(
            f'The video {video_id} has been processed',
            color='green', sections=['KALTURA', 'CONCEPT DETECTION', 'SUCCESS']
        )
    piper_cursor.close()
    if close_connection:
        piper_connection.close()


def fix_subtitles_translation_on_rcp(
        kaltura_ids: list, graph_ai_server='http://127.0.0.1:28800', piper_mysql_json_file=None, piper_connection=None,
        source_language='fr', target_language='en'
):
    close_connection = piper_connection is None
    piper_connection, piper_cursor = get_piper_cursor(
        piper_connection=piper_connection, piper_mysql_json_file=piper_mysql_json_file
    )
    assert source_language != target_language
    if source_language == 'en':
        column_source = 'textEn'
    elif source_language == 'fr':
        column_source = 'textFr'
    else:
        raise NotImplementedError('supported source languages are "en" and "fr"')
    if target_language == 'en':
        column_target = 'textEn'
    elif target_language == 'fr':
        column_target = 'textFr'
    else:
        raise NotImplementedError('supported target languages are "en" and "fr"')
    for video_id in kaltura_ids:
        piper_cursor.execute(f'''
            SELECT 
                segmentId,
                {column_source}
            FROM gen_kaltura.Subtitles WHERE kalturaVideoId="{video_id}" ORDER BY segmentId;
        ''')
        segments_info = list(piper_cursor)
        status_msg(
            f'Translating {len(segments_info)} subtitles '
            f'from {source_language} to {target_language} for kaltura video {video_id}',
            color='grey', sections=['KALTURA', 'FIX TRANSLATION', 'PROCESSING']
        )
        segments_ids = []
        source_text = []
        is_auto_translated = False
        for segments_id, text in segments_info:
            if segments_id == 0:
                if text and text.startswith(default_disclaimer[source_language]):
                    is_auto_translated = True
                    if text == default_disclaimer[source_language]:
                        continue
                    text = '\n'.join(text.split('\n')[1:])
            segments_ids.append(segments_id)
            source_text.append(text)
        translated_text = translate_text(
            source_text, source_language=source_language, target_language=target_language,
            graph_ai_server=graph_ai_server, sections=('KALTURA', 'FIX TRANSLATION', 'TRANSLATE'), force=True
        )
        if is_auto_translated:
            if segments_info[0][1] == default_disclaimer[source_language]:
                segments_ids.insert(0, 0)
                translated_text.insert(0, default_disclaimer[target_language])
            else:
                translated_text[0] = default_disclaimer[target_language] + '\n' + translated_text[0]
        n_fix = 0
        for segment_id, translated_segment in zip(segments_ids, translated_text):
            if translated_segment is None:
                continue
            translated_segment_str = prepare_value_for_mysql(translated_segment.strip(), 'str')
            execute_query(piper_cursor, f'''
                UPDATE gen_kaltura.Subtitles 
                SET {column_target}={translated_segment_str} 
                WHERE kalturaVideoId="{video_id}" AND segmentId={segment_id};
            ''')
            n_fix += 1
        piper_connection.commit()
        status_msg(
            f'{n_fix}/{len(segments_info)} subtitles has been translated from '
            f'{source_language} to {target_language} for video {video_id}',
            color='green', sections=['KALTURA', 'FIX TRANSLATION', 'SUCCESS']
        )
    piper_cursor.close()
    if close_connection:
        piper_connection.close()


def fix_slides_translation_on_rcp(
        kaltura_ids: list, graph_ai_server='http://127.0.0.1:28800', piper_mysql_json_file=None, piper_connection=None,
        source_language='fr', target_language='en'
):
    close_connection = piper_connection is None
    piper_connection, piper_cursor = get_piper_cursor(
        piper_connection=piper_connection, piper_mysql_json_file=piper_mysql_json_file
    )
    assert source_language != target_language
    if source_language == 'en':
        column_source = 'textEn'
    elif source_language == 'fr':
        column_source = 'textFr'
    else:
        raise NotImplementedError('supported source languages are "en" and "fr"')
    if target_language == 'en':
        column_target = 'textEn'
    elif target_language == 'fr':
        column_target = 'textFr'
    else:
        raise NotImplementedError('supported target languages are "en" and "fr"')
    for video_id in kaltura_ids:
        piper_cursor.execute(f'''
                SELECT 
                    slideNumber,
                    {column_source}
                FROM gen_kaltura.Slides WHERE kalturaVideoId="{video_id}" ORDER BY slideNumber;
            ''')
        slides_info = list(piper_cursor)
        status_msg(
            f'Translating {len(slides_info)} slides '
            f'from {source_language} to {target_language} for kaltura video {video_id}',
            color='grey', sections=['KALTURA', 'FIX TRANSLATION', 'PROCESSING']
        )
        slides_id = []
        source_text = []
        for slide_id, text in slides_info:
            slides_id.append(slide_id)
            source_text.append(text)
        translated_text = translate_text(
            source_text, source_language=source_language, target_language=target_language,
            graph_ai_server=graph_ai_server, sections=('KALTURA', 'FIX TRANSLATION', 'TRANSLATE'), force=True
        )
        n_fix = 0
        for slide_id, translated_slide in zip(slides_id, translated_text):
            if translated_slide is None:
                continue
            translated_slide_str = prepare_value_for_mysql(translated_slide.strip(), 'str')
            execute_query(piper_cursor, f'''
                    UPDATE gen_kaltura.Slides
                    SET {column_target}={translated_slide_str} 
                    WHERE kalturaVideoId="{video_id}" AND SlideNumber={slide_id};
                ''')
            n_fix += 1
        piper_connection.commit()
        status_msg(
            f'{n_fix}/{len(slides_info)} slides has been translated from '
            f'{source_language} to {target_language} for video {video_id}',
            color='green', sections=['KALTURA', 'FIX TRANSLATION', 'SUCCESS']
        )
    piper_cursor.close()
    if close_connection:
        piper_connection.close()
