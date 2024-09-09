from datetime import timedelta, datetime
from re import fullmatch
from requests import Session
from isodate import parse_duration
from email.utils import parsedate_to_datetime
from json import dumps
from typing import List, Tuple, Optional
from graphai_client.utils import (
    status_msg, get_video_link_and_size, get_http_header, strfdelta, convert_subtitle_into_segments,
    combine_language_segments, add_initial_disclaimer, default_disclaimer, default_missing_transcript,
    insert_data_into_table, update_data_into_table, execute_query, prepare_value_for_mysql,
    get_piper_connection, get_video_id_and_platform, get_google_resource, GoogleResource, insert_keywords_and_concepts
)
from graphai_client.client import (
    process_video, translate_extracted_text, translate_subtitles, get_fingerprint_of_slides
)
from graphai_client.client_api.utils import login
from graphai_client.client_api.text import clean_text_translate_extract_keywords_and_concepts
from graphai_client.client_api.translation import translate_text
from graphai_client.client_api.embedding import embed_text

language_to_short = {
    'french': 'fr',
    'english': 'en',
    'italian': 'it',
    'german': 'de'
}
short_to_language = {v: k for k, v in language_to_short.items()}


def process_videos_on_rcp(
        video_urls: list, analyze_audio=True, analyze_slides=True, destination_languages=('fr', 'en'), force=False,
        graph_api_json=None, debug=False, piper_mysql_json_file=None, force_download=False, google_api_json=None
):
    login_info = login(graph_api_json)
    youtube_resource = get_google_resource('youtube', google_api_json=google_api_json)
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
        for video_url in video_urls:
            video_id, platform = get_video_id_and_platform(video_url)
            if video_id is None or platform is None:
                status_msg(
                    f'Could not extract the video platform and id from video url: {video_url}',
                    color='yellow', sections=['VIDEO', 'PROCESSING', 'WARNING']
                )
            switchtube_video_id = None
            switchtube_channel = None
            if platform == 'mediaspace':
                switchtube_video_id = kaltura_to_switch_id.get(video_id, None)
                switchtube_channel = switch_ids_to_channel.get(switchtube_video_id, None)
            elif platform == 'switchtube':
                switchtube_video_id = video_id
                switchtube_channel = switch_ids_to_channel.get(switchtube_video_id, None)
            video_information = process_video_on_rcp(
                login_info, piper_connection, youtube_resource, platform, video_id, video_url,
                switchtube_video_id=switchtube_video_id, switch_channel=switchtube_channel,
                analyze_audio=analyze_audio, analyze_slides=analyze_slides, destination_languages=destination_languages,
                force=force, debug=debug, force_download=force_download,
            )
            if video_information is None:
                continue
            if platform is None and video_id is None:
                platform = 'other'
                video_id = video_information['video_token']
            if analyze_slides and (
                    video_information['slides'] is not None or video_information['slides_detected_language'] is not None
            ):
                video_information['slides_detection_time'] = str(datetime.now())
                register_slides(
                    piper_connection, platform, video_id, video_information['slides'],
                    video_information['slides_detected_language']
                )
            if analyze_audio and (
                    video_information['subtitles'] is not None
                    or video_information.get('audio_language', None) is not None
            ):
                video_information['audio_detected_language'] = video_information.get('audio_language', None)
                video_information['audio_transcription_time'] = str(datetime.now())
                register_subtitles(
                    piper_connection, platform, video_id, video_information['subtitles'],
                    video_information['audio_detected_language']
                )
            register_processed_video(piper_connection, platform, video_id, video_information)
            piper_connection.commit()
            status_msg(
                f'The {platform} video {video_id} has been processed',
                color='green', sections=['VIDEO', 'PROCESSING', 'SUCCESS']
            )


def process_video_on_rcp(
        login_info: dict, piper_connection, youtube_resource: GoogleResource,
        platform: Optional[str] = None, video_id: Optional[str] = None, video_url=None,
        switchtube_video_id=None, switch_channel=None,
        analyze_audio=True, analyze_slides=True, destination_languages=('fr', 'en'),
        force=False, debug=False, force_download=False, sections=('VIDEO', 'PROCESSING')
):
    video_details = None
    if platform is None or video_id is None:
        if video_url is None:
            ValueError(f'either both platform and video_id or video_url must be given as argument.')
        video_details = get_downloadable_video_details(
            video_url, [], platform, video_id
        )
    elif platform == 'mediaspace':
        video_details = get_kaltura_video_details(piper_connection, video_id)
    elif platform == 'youtube':
        video_details = get_youtube_video_details(youtube_resource, video_id)
    elif platform == 'switchtube':
        video_details = get_switchtube_video_details(piper_connection, video_id)
    elif platform == 'switchtube (external)':
        video_details = get_downloadable_video_details(
            f'https://tube.switch.ch/external/{video_id}', [], 'switchtube (external)', video_id
        )

    if video_details is None:
        if video_id is not None:
            video_identifier_text = video_id
        else:
            video_identifier_text = f'at {video_url}'
        status_msg(
            f'Details for the video {video_identifier_text} could not be found.',
            color='red', sections=list(sections) + ['ERROR']
        )
        return None
    video_information = get_info_previous_video_processing(piper_connection, platform, video_id)
    for k, v in video_details.items():
        if v is None and video_information.get(k, None) is not None:
            pass
        video_information[k] = v
    video_information['slides'] = None
    video_information['subtitles'] = None
    if analyze_slides:
        if switchtube_video_id is not None and switch_channel is not None:
            # switch video already processed, we can skip slide extraction and OCR
            status_msg(
                f'The video {platform} {video_id} has been found on switchtube as {switchtube_video_id}, '
                'skipping slides detection', color='grey', sections=list(sections) + ['PROCESSING']
            )
            analyze_slides = False
            video_information['slides_detected_language'], video_information['slides'] = get_slides_from_switchtube(
                piper_connection, switch_channel, switchtube_video_id,
                login_info, destination_languages, force, debug
            )
    detect_audio_language = False
    if analyze_audio:
        if platform == 'mediaspace':
            video_information['subtitles'] = get_subtitles_from_kaltura(
                video_id, login_info, piper_connection=piper_connection, force=force,
                destination_languages=destination_languages, debug=debug
            )
        elif platform == 'youtube' and video_information['youtube_caption']:
            # no caption download using the API key, need Oath2
            # video_information['subtitles'] = get_subtitles_from_youtube(video_id, youtube_resource)
            pass
        if video_information['subtitles']:
            status_msg(
                f'Subtitles for the {platform} video {video_id} are present on the platform, '
                'skipping transcription', color='grey', sections=list(sections) + ['PROCESSING']
            )
            analyze_audio = False
            detect_audio_language = True
    new_video_information = process_video(
        video_information['url'], analyze_audio=analyze_audio, analyze_slides=analyze_slides,
        detect_audio_language=detect_audio_language, destination_languages=destination_languages,
        login_info=login_info, force=force, debug=debug, force_download=force_download
    )
    if new_video_information is None:
        status_msg(
            f'processing of video {video_id} on {platform} failed',
            color='red', sections=list(sections) + ['ERROR']
        )
        return None
    video_information['video_size'] = new_video_information['video_size']
    video_information['video_token'] = new_video_information['video_token']
    try:
        audio_stream_idx_largest_bitrate = max({
            idx: stream['bit_rate']
            for idx, stream in enumerate(new_video_information['streams']) if stream['codec_type'] == 'audio'
        })
        audio_stream = new_video_information['streams'][audio_stream_idx_largest_bitrate]
        video_information['audio_bit_rate'] = audio_stream['bit_rate']
        video_information['audio_codec_name'] = audio_stream['codec_name']
        video_information['audio_duration'] = audio_stream['duration']
        video_information['audio_sample_rate'] = audio_stream['sample_rate']
    except ValueError:
        video_information['audio_bit_rate'] = None
        video_information['audio_codec_name'] = None
        video_information['audio_duration'] = None
        video_information['audio_sample_rate'] = None
    try:
        video_stream_idx_largest_bitrate = max({
            idx: stream['bit_rate']
            for idx, stream in enumerate(new_video_information['streams']) if stream['codec_type'] == 'video'
        })
        video_stream = new_video_information['streams'][video_stream_idx_largest_bitrate]
        video_information['video_bit_rate'] = video_stream['bit_rate']
        video_information['video_codec_name'] = video_stream['codec_name']
        video_information['video_duration'] = video_stream['duration']
        video_information['video_resolution'] = video_stream['resolution']
    except ValueError:
        video_information['video_bit_rate'] = None
        video_information['video_codec_name'] = None
        video_information['video_duration'] = None
        video_information['video_resolution'] = None
    if not video_information['ms_duration']:
        if video_information['video_duration']:
            video_information['ms_duration'] = int(float(video_information['video_duration']) * 1000)
        if not video_information['ms_duration']:
            video_information['ms_duration'] = int(float(video_information['audio_duration']) * 1000)
        elif video_information['audio_duration']:
            if video_information['video_duration'] < video_information['audio_duration']:
                video_information['ms_duration'] = int(float(video_information['audio_duration']) * 1000)
    if analyze_audio:
        video_information['subtitles'] = new_video_information.get('subtitles', None)
    if analyze_audio or detect_audio_language:
        video_information['audio_language'] = new_video_information['audio_language']
        video_information['audio_fingerprint'] = new_video_information['audio_fingerprint']
    if analyze_slides:
        video_information['slides_detected_language'] = new_video_information.get('slides_language', None)
        video_information['slides'] = new_video_information.get('slides', None)
    return video_information


def get_kaltura_video_details(db, kaltura_video_id):
    video_details = execute_query(
        db, f'''
            SELECT 
                k.parentEntryId,
                k.downloadUrl AS kaltura_url,
                k.thumbnailUrl,
                k.createdAt AS kalturaCreationTime,
                k.UpdatedAt AS kalturaUpdateTime,
                k.name as title,
                k.description,
                k.userId AS kalturaOwner,
                k.creatorId AS kalturaCreator,
                k.tags,
                k.startDate,
                k.endDate,
                k.msDuration
            FROM ca_kaltura.Videos AS k
            WHERE k.kalturaVideoID="{kaltura_video_id}";'''
    )
    if len(video_details) == 0:
        status_msg(
            f'Skipping video {kaltura_video_id} as it does not exists in ca_kaltura.Videos.',
            color='yellow', sections=['KALTURA', 'VIDEO', 'WARNING']
        )
        return None
    (
        parent_video_id, kaltura_url_api, thumbnail_url, kaltura_creation_time, kaltura_update_time, title,
        description, kaltura_owner, kaltura_creator, tags, start_date, end_date, ms_duration
    ) = video_details[0]
    if not kaltura_url_api:
        status_msg(
            f'Skipping video {kaltura_video_id} which has no download link',
            color='yellow', sections=['KALTURA', 'VIDEO', 'WARNING']
        )
        return None
    if kaltura_url_api.startswith('https://www.youtube.com') \
            or kaltura_url_api.startswith('https://www.youtu.be'):
        kaltura_url = kaltura_url_api
        octet_size = None
    else:
        kaltura_url, octet_size = get_video_link_and_size(kaltura_url_api)
        if kaltura_url is None:
            status_msg(
                f'The video at {kaltura_url_api} is not accessible',
                color='yellow', sections=['KALTURA', 'VIDEO', 'WARNING']
            )
            return None
    return dict(
        platform='mediaspace', video_id=kaltura_video_id, parent_video_id=parent_video_id, url=kaltura_url,
        thumbnail_url=thumbnail_url, video_creation_time=kaltura_creation_time, video_update_time=kaltura_update_time,
        title=title, description=description, owner=kaltura_owner, creator=kaltura_creator, tags=tags,
        ms_duration=ms_duration, video_size=octet_size, start_date=start_date, end_date=end_date,
    )


def get_youtube_video_details(youtube_resource: GoogleResource, youtube_video_id):
    videos = youtube_resource.videos()
    channels = youtube_resource.channels()
    # see https://developers.google.com/youtube/v3/docs/videos/list for the list of properties
    video_request = videos.list(id=youtube_video_id, part='snippet,contentDetails,status')
    video_info_items = video_request.execute()['items']
    if len(video_info_items) == 0:
        return None
    if len(video_info_items) > 1:
        raise RuntimeError(f'got several videos info for youtube video {youtube_video_id}: {video_info_items}')
    video_info = video_info_items[0]
    video_snippet = video_info['snippet']
    video_content_details = video_info['contentDetails']
    video_url = 'https://www.youtube.com/watch?v=' + youtube_video_id
    try:
        thumbnail_url = video_snippet['thumbnails']['maxres']['url']
    except KeyError:
        thumbnail_url = None
    video_creation_time = datetime.fromisoformat(video_snippet.get('publishedAt').replace('Z', '+00:00'))
    title = video_snippet.get('title', None)
    description = video_snippet.get('description')
    tags = video_snippet.get('tags', None)
    if tags:
        tags = ','.join([f'"{tag}"' for tag in tags])
    duration = video_content_details.get('duration', None)
    if duration:
        ms_duration = int(parse_duration(duration).total_seconds() * 1000)
    else:
        ms_duration = None
    youtube_caption = True if video_content_details.get('caption', 'false') == 'true' else False
    video_channel_id = video_snippet.get('channelId', None)
    if video_channel_id:
        channel_request = channels.list(id=video_channel_id, part='snippet')
        channel_info_items = channel_request.execute()['items']
        assert len(channel_info_items) == 1
        channel_info = channel_info_items[0]
        video_owner = channel_info['snippet'].get('customUrl', None)
    else:
        video_owner = None
    return dict(
        platform='youtube', video_id=youtube_video_id, parent_video_id=None, url=video_url, thumbnail_url=thumbnail_url,
        video_creation_time=video_creation_time, video_update_time=video_creation_time, title=title,
        description=description, owner=video_owner, creator=video_owner, tags=tags,
        ms_duration=ms_duration, video_size=None, start_date=None, end_date=None, youtube_caption=youtube_caption
    )


def get_downloadable_video_details(url, alternate_urls=(), platform=None, video_id=None):
    for u in [url] + list(alternate_urls):
        video_url, video_size = get_video_link_and_size(u)
        headers = get_http_header(u)
        video_update_time = None
        if headers:
            video_update_time_str = headers.get('Last-Modified', None)
            if video_update_time_str:
                video_update_time = parsedate_to_datetime(video_update_time_str)
        if video_url is not None:
            return dict(
                platform=platform, video_id=video_id, parent_video_id=None, url=video_url, thumbnail_url=None,
                video_creation_time=video_update_time, video_update_time=video_update_time,
                title=None, description=None, owner=None, creator=None,
                tags=None, ms_duration=None, video_size=video_size, start_date=None, end_date=None
            )
    return None


def get_switchtube_video_details(db, video_id: str):
    kaltura_video_info = execute_query(
        db, f'SELECT kalturaVideoId FROM ca_kaltura.Videos WHERE referenceId="{video_id}";'
    )
    if len(kaltura_video_info) == 0:
        return get_downloadable_video_details(
            f'https://tube.switch.ch/download/video/{video_id}',
            [f'https://tube.switch.ch/videos/{video_id}'],
            platform='switchtube', video_id=video_id
        )
    return get_kaltura_video_details(db, kaltura_video_info[0][0])


def get_slides_from_switchtube(db, switch_channel, switch_video_id, login_info, destination_languages, force, debug):
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


def register_subtitles(
        db, platform, video_id, subtitles, audio_detected_language,
        sections=('VIDEO', 'PROCESSING', 'REGISTER SUBTITLES')
):
    if subtitles is None:
        return
    data_subtitles = []
    num_segment_orig = 0
    num_segment_french = 0
    num_segment_english = 0
    for idx, segment in enumerate(subtitles):
        segment_orig = segment.get(audio_detected_language, None)
        segment_french = segment.get('fr', None)
        segment_english = segment.get('en', None)
        data_subtitles.append(
            [
                platform, video_id, idx, int(segment['start'] * 1000), int(segment['end'] * 1000),
                strfdelta(timedelta(seconds=segment['start']), '{H:02}:{M:02}:{S:02}.{m:03}'),
                strfdelta(timedelta(seconds=segment['end']), '{H:02}:{M:02}:{S:02}.{m:03}'),
                segment_french, segment_english, segment_orig
            ]
        )
        if segment_orig:
            num_segment_orig += 1
        if segment_french:
            num_segment_french += 1
        if segment_english:
            num_segment_english += 1
    execute_query(
        db,
        f'DELETE FROM `gen_video`.`Subtitles` WHERE platform="{platform}" AND videoId="{video_id}"'
    )
    insert_data_into_table(
        db, 'gen_video', 'Subtitles',
        [
            'platform', 'videoId', 'segmentId', 'startMilliseconds', 'endMilliseconds',
            'startTime', 'endTime', 'textFr', 'textEn', 'textOriginal'
        ],
        data_subtitles
    )
    msg = f'registered {num_segment_orig} subtitles in {audio_detected_language}'
    translations = []
    if num_segment_french > 0 and audio_detected_language != 'fr':
        translations.append(
            f'French{" (" + str(num_segment_french) + ")" if num_segment_french != num_segment_orig else ""}'
        )
    if num_segment_english > 0 and audio_detected_language != 'en':
        translations.append(
            f'English{" (" + str(num_segment_english) + ")" if num_segment_english != num_segment_orig else ""}'
        )
    if translations:
        msg += f' and the translation in {" and ".join(translations)}'
    msg += f' for video {video_id} on {platform}'
    status_msg(msg, color='green', sections=list(sections) + ['SUCCESS'])


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
    msg = f'registered {num_slide_orig} slides in {slides_detected_language}'
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


def register_processed_video(db, platform, video_id, video_info, sections=('VIDEO', 'PROCESSING', 'REGISTER VIDEO')):
    execute_query(
        db, f'DELETE FROM `gen_video`.`Videos` WHERE platform="{platform}"AND videoId="{video_id}"'
    )
    insert_data_into_table(
        db, 'gen_video', 'Videos',
        [
            'platform', 'videoId', 'parentVideoId', 'videoToken',
            'audioFingerprint', 'videoUrl', 'thumbnailUrl',
            'videoCreationTime', 'videoUpdateTime',
            'title', 'description', 'owner', 'creator',
            'tags', 'msDuration', 'octetSize',
            'audioBitRate', 'audioCodecName', 'audioDuration', 'audioSampleRate',
            'videoBitRate', 'videoCodecName', 'videoDuration', 'videoResolution',
            'startDate', 'endDate',
            'slidesDetectedLanguage', 'audioDetectedLanguage',
            'slidesDetectionTime', 'audioTranscriptionTime',
            'slidesConceptExtractionTime', 'subtitlesConceptExtractionTime'
        ],
        [(
            platform, video_id, video_info['parent_video_id'], video_info['video_token'],
            video_info['audio_fingerprint'], video_info['url'], video_info['thumbnail_url'],
            video_info['video_creation_time'], video_info['video_update_time'],
            video_info['title'], video_info['description'], video_info['owner'], video_info['creator'],
            video_info['tags'], video_info['ms_duration'], video_info['video_size'],
            video_info['audio_bit_rate'], video_info['audio_codec_name'], video_info['audio_duration'],
            video_info['audio_sample_rate'],
            video_info['video_bit_rate'], video_info['video_codec_name'], video_info['video_duration'],
            video_info['video_resolution'],
            video_info['start_date'], video_info['end_date'],
            video_info['slides_detected_language'], video_info['audio_detected_language'],
            video_info['slides_detection_time'], video_info['audio_transcription_time'],
            video_info['slides_concept_extract_time'], video_info['subtitles_concept_extract_time']
        )]
    )
    status_msg(
        f'Register video info for {video_id} on {platform}', color='green', sections=list(sections) + ['SUCCESS']
    )


def get_info_previous_video_processing(db, platform, video_id):
    # get details about the previous analysis if it exists
    parent_video_id = None
    slides_detected_language = None
    audio_detected_language = None
    slides_detection_time = None
    audio_transcription_time = None
    audio_fingerprint = None
    slides_concept_extract_time = None
    subtitles_concept_extract_time = None
    video_token = None
    previous_analysis_info = execute_query(
        db, f'''SELECT 
            parentVideoId,
            videoToken,
            slidesDetectedLanguage, 
            audioDetectedLanguage, 
            slidesDetectionTime, 
            audioTranscriptionTime,
            audioFingerprint,
            slidesConceptExtractionTime,
            subtitlesConceptExtractionTime
        FROM `gen_video`.`Videos` 
        WHERE platform="{platform}" AND videoId="{video_id}"'''
    )
    if previous_analysis_info:
        (
            parent_video_id, video_token, slides_detected_language, audio_detected_language, slides_detection_time,
            audio_transcription_time, audio_fingerprint, slides_concept_extract_time, subtitles_concept_extract_time
        ) = previous_analysis_info[-1]
    return dict(
        parent_video_id=parent_video_id,
        video_token=video_token,
        slides_detected_language=slides_detected_language,
        audio_detected_language=audio_detected_language,
        slides_detection_time=slides_detection_time,
        audio_transcription_time=audio_transcription_time,
        audio_fingerprint=audio_fingerprint,
        slides_concept_extract_time=slides_concept_extract_time,
        subtitles_concept_extract_time=subtitles_concept_extract_time,
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


def get_subtitles_from_youtube(video_id: str, youtube_resource: GoogleResource):
    captions = youtube_resource.captions()
    captions_request = captions.list(part='snippet,id', videoId=video_id)
    for captions_item in captions_request.execute()['items']:
        captions_snippet = captions_item['snippet']
        if captions_snippet['trackKind'].lower() == 'asr':
            continue
        caption_dl_request = captions.download(id=captions_item['id'])
        caption_dl_response = caption_dl_request.execute()
        print(caption_dl_response)
    subtitles = None
    return subtitles


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
                    now = str(datetime.now())
                    execute_query(
                        piper_connection,
                        f'''UPDATE `gen_video`.`Videos` 
                        SET `slidesConceptExtractionTime`="{now}"  
                        WHERE platform="{platform}" AND videoId="{video_id}"'''
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


def detect_concept_from_publications_on_rcp(
        publication_ids: List[int], graph_api_json=None, login_info=None, piper_mysql_json_file=None
):
    if login_info is None or 'token' not in login_info:
        login_info = login(graph_api_json)
    with Session() as session:
        with get_piper_connection(piper_mysql_json_file) as piper_connection:
            publications_info = execute_query(
                piper_connection,
                f"""SELECT 
                    PublicationID, 
                    Title,
                    Abstract
                FROM gen_infoscience.Publications_tmp AS p
                WHERE PublicationID IN ({', '.join([str(p_id) for p_id in publication_ids])});"""
            )
            for pub_id, title, abstract in publications_info:
                keywords_and_concepts = clean_text_translate_extract_keywords_and_concepts(
                    text_data=(title, abstract), login_info=login_info, session=session
                )
                insert_keywords_and_concepts(
                    piper_connection, pk=(pub_id,), keywords_and_concepts=keywords_and_concepts,
                    schemas_keyword='gen_infoscience', table_keywords='Publications_tmp',
                    pk_columns_keywords=('PublicationID',), schemas_concepts='gen_infoscience',
                    table_concepts='Publication_to_Page_Mapping_tmp', pk_columns_concepts=('PublicationID',),
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


def detect_concept_from_persons_on_rcp(
        scipers: List[int], graph_api_json=None, login_info=None, piper_mysql_json_file=None
):
    if login_info is None or 'token' not in login_info:
        login_info = login(graph_api_json)
    with Session() as session:
        with get_piper_connection(piper_mysql_json_file) as piper_connection:
            persons_info = execute_query(
                piper_connection,
                f"""SELECT 
                    p.SCIPER, 
                    p.BiographyEN
                FROM gen_people.Person_Simplified_Info_ISA_tmp as p
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
                    schemas_keyword='gen_people', table_keywords='Person_Simplified_Info_ISA_tmp',
                    pk_columns_keywords=('SCIPER',), schemas_concepts='gen_people',
                    table_concepts='Person_to_Page_Mapping_tmp', pk_columns_concepts=('SCIPER',),
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


def compute_embeddings_of_concepts_on_rcp(
        page_ids: List[int], graph_api_json=None, login_info=None, piper_mysql_json_file=None, batch_size=1000,
        temp_tables=True, max_text_length=400
):
    if login_info is None or 'token' not in login_info:
        login_info = login(graph_api_json)
    num_pages = len(page_ids)
    if num_pages > batch_size:
        for idx in range(0, num_pages, batch_size):
            compute_embeddings_of_concepts_on_rcp(
                page_ids, graph_api_json=graph_api_json, login_info=login_info,
                piper_mysql_json_file=piper_mysql_json_file, batch_size=batch_size
            )
        return
    schema = 'gen_wikipedia'
    page_neighbours_table = 'Pages_Neighbours'
    page_content_table = 'Page_Content_Full'
    if temp_tables:
        page_neighbours_table += '_tmp'
        page_content_table += '_tmp'
    with get_piper_connection(piper_mysql_json_file) as piper_connection:
        concepts_info = execute_query(
            piper_connection,
            f"""SELECT 
                p.PageID, 
                pc.OpeningText
            FROM {schema}.{page_neighbours_table} as p
            INNER JOIN {schema}.{page_content_table} AS pc USING (PageID)
            WHERE p.PageID IN ({', '.join([str(p_id) for p_id in page_ids])});"""
        )
        text_to_embed = []
        page_id_of_embedded_text = []
        for page_id, opening_text in concepts_info:
            page_id_of_embedded_text.append(page_id)
            text_to_embed.append(opening_text)
        embedded_texts = embed_text(text_to_embed, login_info=login_info, max_text_length=max_text_length)
        assert len(embedded_texts) == len(page_id_of_embedded_text)
        data_embeddings = [
            (dumps([round(e, 8) for e in embedding]), page_id)
            for page_id, embedding in zip(page_id_of_embedded_text, embedded_texts)
        ]
        update_data_into_table(
            piper_connection, schema=schema, table_name=page_neighbours_table,
            columns=['Embedding'], pk_columns=['PageID'], data=data_embeddings
        )
        piper_connection.commit()
    status_msg(
        f'Embeddings have been computed for {num_pages} concepts in the ontology neighbourhood', color='green',
        sections=['GRAPHAI', 'EMBED CONCEPT', 'SUCCESS']
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
            video_token, video_size, streams = get_video_token(
                video_url, login_info, force=force_download, sections=('KALTURA', 'DOWNLOAD VIDEO')
            )
            codec_types = [s['codec_type'] for s in streams]
            if not video_token:
                status_msg(
                    f'Skipping video {video_id} as the download failed.',
                    color='red', sections=['KALTURA', 'FINGERPRINT', 'FAILED']
                )
                continue
            if 'video' not in codec_types:
                status_msg(
                    f'Skipping slide fingerprinting for video {video_id} as it does not contains a video stream.',
                    color='yellow', sections=['KALTURA', 'FINGERPRINT', 'SLIDES', 'WARNING']
                )
                slides = None
            else:
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
            if 'audio' not in codec_types:
                status_msg(
                    f'Skipping audio fingerprinting for video {video_id} as it does not contains an audio stream.',
                    color='yellow', sections=['KALTURA', 'FINGERPRINT', 'AUDIO', 'WARNING']
                )
                audio_token = None
            else:
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
            new_audio_fingerprint = None
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
            # update db
            if update_data_slides:
                update_data_into_table(
                    piper_connection, schema='gen_kaltura', table_name='Slides',
                    columns=['fingerprint'], pk_columns=['kalturaVideoId', 'SlideNumber'], data=update_data_slides
                )
            if new_audio_fingerprint is not None:
                update_data_into_table(
                    piper_connection, schema='gen_kaltura', table_name='Videos',
                    columns=['audioFingerprint'], pk_columns=['kalturaVideoId'],
                    data=[(new_audio_fingerprint, video_id)]
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
