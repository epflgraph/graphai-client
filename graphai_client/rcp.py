from datetime import timedelta, datetime
from re import fullmatch
from requests import Session
from graphai_client.utils import (
    status_msg, get_video_link_and_size, strfdelta, insert_line_into_table_with_types, convert_subtitle_into_segments,
    combine_language_segments, add_initial_disclaimer, default_disclaimer, default_missing_transcript,
    insert_data_into_table_with_type, execute_query, prepare_value_for_mysql, get_piper_connection, execute_many
)
from graphai_client.client import (
    process_video, translate_extracted_text, translate_subtitles, get_fingerprint_of_slides
)
from graphai_client.client_api.utils import login
from graphai_client.client_api.text import extract_concepts_from_text
from graphai_client.client_api.translation import translate_text

language_to_short = {
    'french': 'fr',
    'english': 'en',
    'italian': 'it',
    'german': 'de'
}
short_to_language = {v: k for k, v in language_to_short.items()}


def process_videos_on_rcp(
        kaltura_ids: list, analyze_audio=True, analyze_slides=True, destination_languages=('fr', 'en'), force=False,
        graph_api_json=None, login_info=None, debug=False, piper_mysql_json_file=None, force_download=False
):
    if login_info is None or 'token' not in login_info:
        login_info = login(graph_api_json)
    with get_piper_connection(piper_mysql_json_file) as piper_connection:
        switch_slide_text_info = execute_query(
            piper_connection, 'SELECT DISTINCT SwitchVideoID, SwitchChannelID FROM gen_switchtube.Slide_Text;'
        )
        switch_ids_to_channel_with_text_from_slides = {i[0]: i[1] for i in switch_slide_text_info}
        kaltura_to_switch_info = execute_query(
            piper_connection, '''
            SELECT 
                k.kalturaVideoID,
                m.switchtube_id
            FROM ca_kaltura.Videos AS k 
            LEFT JOIN man_kaltura.Mapping_Kaltura_Switchtube AS m ON m.kaltura_id=k.kalturaVideoId;
        ''')
        kaltura_to_switch_id = {i[0]: i[1] for i in kaltura_to_switch_info}
        for kaltura_video_id in kaltura_ids:
            status_msg(
                f'Processing kaltura video {kaltura_video_id}',
                color='grey', sections=['KALTURA', 'VIDEO', 'PROCESSING']
            )
            video_details = execute_query(
                piper_connection, f'''
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
                WHERE k.kalturaVideoID="{kaltura_video_id}";'''
            )
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
            if kaltura_url_api.startswith('https://www.youtube.com'):
                kaltura_url = kaltura_url_api
                octet_size = None
            else:
                kaltura_url, octet_size = get_video_link_and_size(kaltura_url_api)
                if kaltura_url is None:
                    status_msg(
                        f'The video at {kaltura_url_api} is not accessible',
                        color='yellow', sections=['KALTURA', 'VIDEO', 'WARNING']
                    )
                    continue
            slides = None
            subtitles = None
            # get details about the previous analysis if it exists
            slides_detected_language = None
            audio_detected_language = None
            slides_detection_time = None
            audio_transcription_time = None
            audio_fingerprint = None
            slides_concept_extract_time = None
            subtitles_concept_extract_time = None
            previous_analysis_info = execute_query(
                piper_connection, f'''SELECT 
                    slidesDetectedLanguage, 
                    audioDetectedLanguage, 
                    slidesDetectionTime, 
                    audioTranscriptionTime,
                    audioFingerprint,
                    slidesConceptExtractionTime,
                    subtitlesConceptExtractionTime
                FROM `gen_kaltura`.`Videos` 
                WHERE kalturaVideoId="{kaltura_video_id}"'''
            )
            if previous_analysis_info:
                (
                    slides_detected_language, audio_detected_language, slides_detection_time,
                    audio_transcription_time, audio_fingerprint,
                    slides_concept_extract_time, subtitles_concept_extract_time
                ) = previous_analysis_info[-1]
            # skip OCR if the video was on switchtube and has already OCR results from there
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
                    num_slides_languages = {'en': 0, 'fr': 0}
                    slides_video_info = execute_query(
                        piper_connection, f'''
                        SELECT 
                            t.SlideID,
                            SUBSTRING(t.SlideID,LENGTH(t.SwitchChannelID) + LENGTH(t.SwitchVideoID) + 3), 
                            t.SlideText,
                            SUM(IF(o.DetectedLanguage='fr', 1, 0)) AS Nfr,
                            SUM(IF(o.DetectedLanguage='en', 1, 0)) AS Nen
                        FROM gen_switchtube.Slide_Text AS t
                        LEFT JOIN gen_switchtube.Slide_OCR AS o ON o.SlideID=t.SlideID AND Method='google (dtd)'
                        WHERE SwitchChannelID='{switch_channel}' AND SwitchVideoID='{switchtube_video_id}' 
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
                if analyze_audio:
                    subtitles = get_subtitles_from_kaltura(
                        kaltura_video_id, login_info, piper_connection=piper_connection, force=force,
                        destination_languages=destination_languages, debug=debug
                    )
                    if subtitles:
                        video_information = process_video(
                            kaltura_url, analyze_audio=False, analyze_slides=False, force=force,
                            detect_audio_language=True, audio_language=None,
                            login_info=login_info, debug=debug, force_download=force_download
                        )
                    else:
                        video_information = process_video(
                            kaltura_url, analyze_audio=True, analyze_slides=False, force=force,
                            destination_languages=destination_languages, audio_language=None,
                            login_info=login_info, debug=debug, force_download=force_download
                        )
                        subtitles = video_information.get('subtitles', None)
                    audio_fingerprint = video_information.get('audio_fingerprint', None)
            else:  # full processing of the video
                subtitles = get_subtitles_from_kaltura(
                    kaltura_video_id, login_info, piper_connection=piper_connection, force=force,
                    destination_languages=destination_languages, debug=debug
                )
                if subtitles and analyze_audio:
                    video_information = process_video(
                        kaltura_url, analyze_audio=False, analyze_slides=analyze_slides, force=force,
                        detect_audio_language=True, audio_language=None,
                        login_info=login_info, debug=debug, force_download=force_download
                    )
                else:
                    video_information = process_video(
                        kaltura_url, analyze_audio=analyze_audio, analyze_slides=analyze_slides, force=force,
                        destination_languages=destination_languages, login_info=login_info, debug=debug,
                        force_download=force_download
                    )
                    subtitles = video_information.get('subtitles', None)
                slides = video_information.get('slides', None)
                if analyze_slides:
                    slides_detected_language = video_information.get('slides_language', None)
                if analyze_audio:
                    audio_fingerprint = video_information.get('audio_fingerprint', None)
            # update gen_kaltura with processed info
            if analyze_slides and slides is not None:
                slides_detection_time = str(datetime.now())
                data_slides = []
                for slide_number, slide in enumerate(slides):
                    slide_time = strfdelta(timedelta(seconds=slide['timestamp']), '{H:02}:{M:02}:{S:02}')
                    data_slides.append(
                        [
                            kaltura_video_id, slide_number, slide['fingerprint'],
                            slide['timestamp'], slide_time,
                            slide.get('fr', None), slide.get('en', None), slide.get(slides_detected_language, None)
                        ]
                    )
                execute_query(
                    piper_connection, f'DELETE FROM `gen_kaltura`.`Slides` WHERE kalturaVideoId="{kaltura_video_id}"'
                )
                insert_data_into_table_with_type(
                    piper_connection, 'gen_kaltura', 'Slides',
                    [
                        'kalturaVideoId', 'slideNumber', 'fingerprint',
                        'timestamp', 'slideTime',
                        'textFr', 'textEn', 'textOriginal'
                    ],
                    data_slides,
                    [
                        'str', 'int', 'str',
                        'int', 'str',
                        'str', 'str', 'str'
                    ]
                )
            if analyze_audio and subtitles is not None:
                audio_transcription_time = str(datetime.now())
                audio_detected_language = video_information.get('audio_language', None)
                data_subtitles = []
                for idx, segment in enumerate(subtitles):
                    data_subtitles.append(
                        [
                            kaltura_video_id, idx, int(segment['start'] * 1000), int(segment['end'] * 1000),
                            strfdelta(timedelta(seconds=segment['start']), '{H:02}:{M:02}:{S:02}.{m:03}'),
                            strfdelta(timedelta(seconds=segment['end']), '{H:02}:{M:02}:{S:02}.{m:03}'),
                            segment.get('fr', None), segment.get('en', None),
                            segment.get(audio_detected_language, None)
                        ]
                    )
                execute_query(
                    piper_connection, f'DELETE FROM `gen_kaltura`.`Subtitles` WHERE kalturaVideoId="{kaltura_video_id}"'
                )
                insert_data_into_table_with_type(
                    piper_connection, 'gen_kaltura', 'Subtitles',
                    [
                        'kalturaVideoId', 'segmentId', 'startMilliseconds', 'endMilliseconds',
                        'startTime', 'endTime',
                        'textFr', 'textEn', 'textOriginal'
                    ],
                    data_subtitles,
                    [
                        'str', 'int', 'int', 'int',
                        'str', 'str',
                        'str', 'str', 'str'
                    ]
                )
            if analyze_audio and subtitles is None:
                audio_transcription_time = str(datetime.now())
                audio_detected_language = None
            video_size = video_information.get('video_size', octet_size)
            execute_query(
                piper_connection, f'DELETE FROM `gen_kaltura`.`Videos` WHERE kalturaVideoId="{kaltura_video_id}"'
            )
            insert_line_into_table_with_types(
                piper_connection, 'gen_kaltura', 'Videos',
                [
                    'kalturaVideoId', 'audioFingerprint', 'kalturaUrl', 'thumbnailUrl', 'kalturaCreationTime',
                    'kalturaUpdateTime', 'title', 'description', 'kalturaOwner', 'kalturaCreator', 'tags',
                    'categories', 'kalturaEntitledEditors', 'msDuration', 'octetSize',
                    'slidesDetectedLanguage', 'audioDetectedLanguage', 'switchVideoId',
                    'slidesDetectionTime', 'audioTranscriptionTime',
                    'slidesConceptExtractionTime', 'subtitlesConceptExtractionTime'
                ],
                [
                    kaltura_video_id, audio_fingerprint, kaltura_url, thumbnail_url, kaltura_creation_time,
                    kaltura_update_time, title, description, kaltura_owner, kaltura_creator, tags,
                    categories, kaltura_entitled_editor, ms_duration, video_size,
                    slides_detected_language, audio_detected_language, switchtube_video_id,
                    slides_detection_time, audio_transcription_time,
                    slides_concept_extract_time, subtitles_concept_extract_time
                ],
                [
                    'str', 'str', 'str', 'str', 'str',
                    'str', 'str', 'str', 'str', 'str', 'str',
                    'str', 'str', 'int', 'int',
                    'str', 'str', 'str',
                    'str', 'str',
                    'str', 'str'
                ]
            )
            piper_connection.commit()
            status_msg(
                f'The video {kaltura_video_id} has been processed',
                color='green', sections=['KALTURA', 'VIDEO', 'SUCCESS']
            )


def get_subtitles_from_kaltura(
        kaltura_video_id, login_info, piper_connection=None, piper_mysql_json_file=None, force=False,
        destination_languages=('en', 'fr'), ignore_autogenerated=True, debug=False
):
    close_connection = False
    if piper_connection is None:
        piper_connection = get_piper_connection(piper_mysql_json_file)
        close_connection = True
    subtitle_query = f'''
        SELECT captionData, fileExt, language 
        FROM ca_kaltura.Captions WHERE kalturaVideoId='{kaltura_video_id}'
    '''
    if ignore_autogenerated:
        partner_data_info = execute_query(
            piper_connection,
            f"SELECT partnerData FROM ca_kaltura.Videos WHERE kalturaVideoId='{kaltura_video_id}';"
        )
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
    caption_info = execute_query(piper_connection, subtitle_query)
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
            try:
                segments = convert_subtitle_into_segments(caption_data, file_ext=file_ext)
            except Exception as e:
                status_msg(
                    f'Error parsing the {lang} subtitle for {kaltura_video_id}: {e}',
                    sections=['GRAPHAI', 'GET SUBTITLES', 'WARNING'], color='yellow'
                )
                continue
            if segments and len(segments) == 1 and \
                    segments[0]['text'] == default_missing_transcript.get(lang, None):
                continue
            subtitles_in_kaltura[lang] = segments
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
    if not subtitles_in_kaltura:
        return None
    subtitles = combine_language_segments(**subtitles_in_kaltura, precision_s=2)
    status_msg(
        f'Found subtitles ({len(subtitles)} segments) in kaltura in {", ".join(subtitles_in_kaltura.keys())}',
        color='grey', sections=['GRAPHAI', 'GET SUBTITLES', 'SUCCESS']
    )
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
                translate_from = list(subtitles_in_kaltura.keys())[0]
            status_msg(
                f'translate transcription for {len(subtitles)} segments in {translate_from}',
                color='grey', sections=['GRAPHAI', 'TRANSLATE', 'PROCESSING']
            )
            subtitles = translate_subtitles(
                subtitles, login_info, force=force, source_language=translate_from,
                destination_languages=missing_destination_language, debug=debug
            )
            subtitles = add_initial_disclaimer(subtitles, restrict_lang=missing_destination_language)
    if close_connection:
        piper_connection.close()
    return subtitles


def detect_concept_on_rcp(
        kaltura_ids: list, analyze_subtitles=False, analyze_slides=True, graph_api_json=None, login_info=None,
        piper_mysql_json_file=None
):
    if login_info is None or 'token' not in login_info:
        login_info = login(graph_api_json)
    with Session() as session:
        with get_piper_connection(piper_mysql_json_file) as piper_connection:
            for video_id in kaltura_ids:
                status_msg(
                    f'Processing kaltura video {video_id}',
                    color='grey', sections=['KALTURA', 'CONCEPT DETECTION', 'PROCESSING']
                )
                if analyze_subtitles:
                    segments_info = execute_query(
                        piper_connection, f'''
                        SELECT 
                            segmentId,
                            textEn
                        FROM gen_kaltura.Subtitles WHERE kalturaVideoId="{video_id}";
                    ''')
                    status_msg(
                        f'Extracting concepts from {len(segments_info)} subtitles of video {video_id}',
                        color='grey', sections=['KALTURA', 'CONCEPT DETECTION', 'SUBTITLES', 'PROCESSING']
                    )
                    execute_query(
                        piper_connection,
                        f'DELETE FROM `gen_kaltura`.`Subtitle_Concepts` WHERE kalturaVideoId="{video_id}";'
                    )
                    segments_processed = 0
                    concepts_segments_data = []
                    for segment_id, segment_text in segments_info:
                        if not segment_text:
                            continue
                        segment_scores = extract_concepts_from_text(
                            segment_text, login_info, sections=['KALTURA', 'CONCEPT DETECTION', 'SUBTITLES'],
                            session=session
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
                        piper_connection, schema='gen_kaltura', table_name='Subtitle_Concepts',
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
                    if segments_processed > 0:
                        status_msg(
                            f'Concepts have been extracted from {segments_processed}/{len(segments_info)} '
                            f'subtitles of {video_id}',
                            color='green', sections=['KALTURA', 'CONCEPT DETECTION', 'SUBTITLES', 'SUCCESS']
                        )
                    else:
                        status_msg(
                            f'No usable subtitles found for video {video_id}',
                            color='yellow', sections=['KALTURA', 'CONCEPT DETECTION', 'SUBTITLES', 'WARNING']
                        )
                    now = str(datetime.now())
                    execute_query(
                        piper_connection,
                        f'''UPDATE `gen_kaltura`.`Videos` 
                        SET `subtitlesConceptExtractionTime`="{now}" 
                        WHERE kalturaVideoId="{video_id}"'''
                    )
                    piper_connection.commit()
                if analyze_slides:
                    slides_info = execute_query(
                        piper_connection, f'''
                        SELECT 
                            slideNumber,
                            textEn
                        FROM gen_kaltura.Slides WHERE kalturaVideoId="{video_id}";
                    ''')
                    status_msg(
                        f'Extracting concepts from {len(slides_info)} slides of video {video_id}',
                        color='grey', sections=['KALTURA', 'CONCEPT DETECTION', 'SLIDES', 'PROCESSING']
                    )
                    execute_query(
                        piper_connection,
                        f'DELETE FROM `gen_kaltura`.`Slide_Concepts` WHERE kalturaVideoId="{video_id}";'
                    )
                    slides_processed = 0
                    concepts_slides_data = []
                    for slide_number, slide_text in slides_info:
                        if not slide_text:
                            continue
                        slide_scores = extract_concepts_from_text(
                            slide_text, login_info, sections=['KALTURA', 'CONCEPT DETECTION', 'SLIDES']
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
                        piper_connection, schema='gen_kaltura', table_name='Slide_Concepts',
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
                        status_msg(
                            f'len{concepts_slides_data} concepts have been extracted '
                            f'from {slides_processed}/{len(slides_info)} slides of {video_id}',
                            color='green', sections=['KALTURA', 'CONCEPT DETECTION', 'SLIDES', 'SUCCESS']
                        )
                    else:
                        status_msg(
                            f'No usable slides found for video {video_id}',
                            color='yellow', sections=['KALTURA', 'CONCEPT DETECTION', 'SLIDES', 'WARNING']
                        )
                    now = str(datetime.now())
                    execute_query(
                        piper_connection,
                        f'''UPDATE `gen_kaltura`.`Videos` 
                            SET `slidesConceptExtractionTime`="{now}" 
                            WHERE kalturaVideoId="{video_id}";'''
                    )
                    piper_connection.commit()
                status_msg(
                    f'The video {video_id} has been processed',
                    color='green', sections=['KALTURA', 'CONCEPT DETECTION', 'SUCCESS']
                )


def fix_subtitles_translation_on_rcp(
        kaltura_ids: list, graph_api_json=None, piper_mysql_json_file=None, source_language='fr', target_language='en'
):
    login_info = login(graph_api_json)
    assert source_language != target_language
    with get_piper_connection(piper_mysql_json_file) as piper_connection:
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
            segments_info = execute_query(
                piper_connection, f'''
                SELECT 
                    segmentId,
                    {column_source}
                FROM gen_kaltura.Subtitles WHERE kalturaVideoId="{video_id}" ORDER BY segmentId;'''
            )
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
                login_info=login_info, sections=('KALTURA', 'FIX TRANSLATION', 'TRANSLATE'), force=True
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
                execute_query(piper_connection, f'''
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


def fix_slides_translation_on_rcp(
        kaltura_ids: list, graph_api_json=None, piper_mysql_json_file=None,
        source_language='fr', target_language='en'
):
    login_info = login(graph_api_json)
    assert source_language != target_language
    with get_piper_connection(piper_mysql_json_file) as piper_connection:
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
            slides_info = execute_query(
                piper_connection, f'''
                SELECT 
                    slideNumber,
                    {column_source}
                FROM gen_kaltura.Slides WHERE kalturaVideoId="{video_id}" ORDER BY slideNumber;'''
            )
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
                login_info=login_info, sections=('KALTURA', 'FIX TRANSLATION', 'TRANSLATE'), force=True
            )
            n_fix = 0
            for slide_id, translated_slide in zip(slides_id, translated_text):
                if translated_slide is None:
                    continue
                translated_slide_str = prepare_value_for_mysql(translated_slide.strip(), 'str')
                execute_query(
                    piper_connection, f'''
                    UPDATE gen_kaltura.Slides
                    SET {column_target}={translated_slide_str} 
                    WHERE kalturaVideoId="{video_id}" AND SlideNumber={slide_id};'''
                )
                n_fix += 1
            piper_connection.commit()
            status_msg(
                f'{n_fix}/{len(slides_info)} slides has been translated from '
                f'{source_language} to {target_language} for video {video_id}',
                color='green', sections=['KALTURA', 'FIX TRANSLATION', 'SUCCESS']
            )


def fingerprint_on_rcp(
        kaltura_ids: list, graph_api_json=None, piper_mysql_json_file=None, force_download=True, force=False,
        debug=False
):
    from graphai_client.client_api.video import get_video_token, extract_slides, extract_audio
    from graphai_client.client_api.voice import calculate_fingerprint as calculate_audio_fingerprint

    login_info = login(graph_api_json)
    with get_piper_connection(piper_mysql_json_file) as piper_connection:
        videos_id_str = ', '.join([f'"{video_id}"' for video_id in kaltura_ids])
        video_info = execute_query(piper_connection, f'''
            SELECT kalturaVideoId, kalturaUrl, audioFingerprint 
            FROM gen_kaltura.Videos 
            WHERE kalturaVideoId IN ({videos_id_str});
        ''')
        for video_id, video_url, existing_audio_fingerprint in video_info:
            status_msg(
                f'Processing video {video_id}...', color='grey', sections=['KALTURA', 'FINGERPRINT', 'PROCESSING']
            )
            # get existing slides info
            existing_slides_timestamp = {}
            existing_slides_fingerprint = {}
            slides_info = execute_query(piper_connection, f'''
                SELECT slideNumber, `timestamp`, fingerprint 
                FROM gen_kaltura.Slides 
                WHERE kalturaVideoId="{video_id}" 
                ORDER BY slideNumber;
            ''')
            for slide_num, timestamp, slide_fingerprint in slides_info:
                existing_slides_timestamp[slide_num] = timestamp
                existing_slides_fingerprint[slide_num] = slide_fingerprint
            num_existing_slides = len(existing_slides_timestamp)
            # extract slides fingerprint
            video_token, video_size = get_video_token(
                video_url, login_info, force=force_download, sections=('KALTURA', 'DOWNLOAD VIDEO')
            )
            if not video_token:
                status_msg(
                    f'Skipping video {video_id} as the download failed.',
                    color='red', sections=['KALTURA', 'FINGERPRINT', 'FAILED']
                )
                continue
            slides = extract_slides(
                video_token, login_info, recalculate_cached=True, sections=('KALTURA', 'EXTRACT SLIDES')
            )
            if not slides:
                status_msg(
                    'failed to extract slides based on cached results, forcing extraction',
                    color='yellow', sections=['KALTURA', 'EXTRACT SLIDES', 'WARNING']
                )
                slides = extract_slides(
                    video_token, login_info, force=True, sections=('KALTURA', 'EXTRACT SLIDES')
                )
            new_slides_fingerprint_per_timestamp = {}
            if slides:
                num_slides_in_cache = len(slides)
                slides = get_fingerprint_of_slides(slides, login_info, force=force, debug=debug)
                for slide in slides.values():
                    fingerprint = slide.get('fingerprint', None)
                    if fingerprint:
                        new_slides_fingerprint_per_timestamp[slide['timestamp']] = fingerprint
                num_slides_fingerprinted = len(new_slides_fingerprint_per_timestamp)
                if num_slides_fingerprinted == num_slides_in_cache:
                    status_msg(
                        f'Fingerprinted {num_slides_fingerprinted} slides for video {video_id}',
                        color='green', sections=['KALTURA', 'FINGERPRINT', 'SLIDES', 'SUCCESS']
                    )
                else:
                    status_msg(
                        f'Fingerprinted only {num_slides_fingerprinted}/{num_slides_in_cache} slides '
                        f'for video {video_id}',
                        color='yellow', sections=['KALTURA', 'FINGERPRINT', 'SLIDES', 'WARNING']
                    )
                # check if existing info and extracted slides matches
                if num_slides_in_cache != num_existing_slides:
                    status_msg(
                        f'The number of slides in the cache: {num_slides_in_cache} does not match that '
                        f'in the database: {num_existing_slides} for video {video_id}',
                        color='yellow', sections=['KALTURA', 'FINGERPRINT', 'SLIDES', 'WARNING']
                    )
            update_data_slides = []
            for existing_slides_num, existing_slides_timestamp in existing_slides_timestamp.items():
                if existing_slides_timestamp in new_slides_fingerprint_per_timestamp:
                    new_slide_fingerprint = new_slides_fingerprint_per_timestamp[existing_slides_timestamp]
                    existing_slide_fingerprint = existing_slides_fingerprint[existing_slides_num]
                    if existing_slide_fingerprint and existing_slide_fingerprint != new_slide_fingerprint:
                        status_msg(
                            f'Computed fingerprint {new_slide_fingerprint} does not match that '
                            f'in the database: {existing_slide_fingerprint} for slide {existing_slides_num} '
                            f'at t={existing_slides_timestamp}s of video {video_id}',
                            color='yellow', sections=['KALTURA', 'FINGERPRINT', 'SLIDES', 'WARNING']
                        )
                    update_data_slides.append((new_slide_fingerprint, video_id, existing_slides_num))
            num_update_slides = len(update_data_slides)
            if num_update_slides != num_existing_slides:
                status_msg(
                    f'Could only match {num_update_slides}/{num_existing_slides} fingerprinted slides '
                    f'with respect to the ones in the database for video {video_id}',
                    color='yellow', sections=['KALTURA', 'FINGERPRINT', 'SLIDES', 'WARNING']
                )
            # extract audio fingerprint
            new_audio_fingerprint = None
            audio_token = extract_audio(
                video_token, login_info, recalculate_cached=True, sections=('KALTURA', 'EXTRACT AUDIO')
            )
            if not audio_token:
                status_msg(
                    'failed to extract audio based on cached result, forcing extraction',
                    color='yellow', sections=['KALTURA', 'EXTRACT AUDIO', 'WARNING']
                )
                audio_token = extract_audio(
                    video_token, login_info, force=True, sections=('KALTURA', 'EXTRACT AUDIO')
                )
            if audio_token:
                new_audio_fingerprint = calculate_audio_fingerprint(
                    audio_token, login_info, sections=['KALTURA', 'FINGERPRINT', 'AUDIO']
                )
                if existing_audio_fingerprint and existing_audio_fingerprint != new_audio_fingerprint:
                    status_msg(
                        f'Computed audio fingerprint {new_audio_fingerprint} does not match that '
                        f'in the database: {existing_audio_fingerprint} for video {video_id}',
                        color='yellow', sections=['KALTURA', 'FINGERPRINT', 'AUDIO', 'WARNING']
                    )
            else:
                new_audio_fingerprint = ''
            # update db
            if update_data_slides:
                execute_many(
                    piper_connection,
                    'UPDATE gen_kaltura.Slides SET fingerprint=%s WHERE kalturaVideoId=%s AND SlideNumber=%s;',
                    update_data_slides
                )
            if new_audio_fingerprint is not None:
                execute_many(
                    piper_connection,
                    'UPDATE gen_kaltura.Videos SET audioFingerprint=%s WHERE kalturaVideoId=%s;',
                    [(new_audio_fingerprint, video_id)]
                )
            piper_connection.commit()
            if num_update_slides > 0 or new_audio_fingerprint:
                status_msg(
                    f'Success fingerprinting {num_update_slides}/{num_existing_slides} slides '
                    f'and {"1" if new_audio_fingerprint else 0} audio for video {video_id}',
                    color='green', sections=['KALTURA', 'FINGERPRINT', 'SUCCESS']
                )
            else:
                status_msg(
                    f'Fingerprinting of slides and audio failed for video {video_id}',
                    color='red', sections=['KALTURA', 'FINGERPRINT', 'FAILED']
                )
