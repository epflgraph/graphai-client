#!/usr/bin/env -S python -u
import sys
from os.path import join, dirname, realpath
from datetime import datetime
from typing import List
from email.utils import parsedate_to_datetime
from isodate import parse_duration
from graphai_client.client_api.utils import login
from graphai_client.utils import (
    status_msg, get_video_link_and_size, get_http_header, execute_query, insert_data_into_table,
    get_piper_connection, get_video_id_and_platform, get_google_resource, GoogleResource,
)
from graphai_client.client import (
    get_audio_fingerprint_of_video, download_url, get_video_information_from_streams
)


def get_video_info_on_rcp(
        video_urls: List[str], piper_mysql_json_file=None, google_api_youtube_json=None, graph_api_json=None,
        sections=('VIDEO', 'GET INFO'), force=False, debug=False, force_download=False
):
    login_info = login(graph_api_json)
    youtube_resource = get_google_resource('youtube', google_api_json=google_api_youtube_json)
    with get_piper_connection(piper_mysql_json_file) as piper_connection:
        for video_url in video_urls:
            status_msg(
                f'Getting info about the video at {video_url}...',
                color='yellow', sections=list(sections) + ['PROCESSING']
            )
            video_id, platform = get_video_id_and_platform(video_url)
            if video_id is None or platform is None:
                status_msg(
                    f'Could not extract the video platform and id from video url: {video_url}',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
            video_details = None
            if platform is None or platform == 'other' or video_id is None:
                if video_url is None:
                    ValueError(f'either both platform and video_id or video_url must be given as argument.')
                video_details = get_downloadable_video_details(
                    video_url, [], 'other', video_id
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
                    video_identifier_text = f'{video_id} on {platform}'
                else:
                    video_identifier_text = f'at {video_url}'
                status_msg(
                    f'Details for the video {video_identifier_text} could not be found.',
                    color='red', sections=list(sections) + ['ERROR']
                )
                continue
            previous_processing_info = get_info_previous_video_processing(
                piper_connection, platform, video_id, video_url
            )
            update_processing_time = False
            if platform is not None and video_id is not None:
                # we update processing times if the duration did not change
                if (
                    previous_processing_info['ms_duration'] is not None
                    and abs(previous_processing_info['ms_duration'] - video_details['ms_duration']) < 2000
                ):
                    update_processing_time = True
                    # if the platform is mediaspace, we update the processing time only if we found a flavor
                    # which has the same size as the video previously analyzed
                    if platform == 'mediaspace' and previous_processing_info['video_size'] is not None:
                        flavors_info = execute_query(
                            piper_connection, f'''
                                SELECT flavorParams, downloadUrl
                                FROM ca_kaltura.Video_Flavors WHERE kalturaVideoId="{video_id}";
                            '''
                        )
                        flavor_previously_analyzed = None
                        for flavor, url in flavors_info:
                            url_verified, video_size = get_video_link_and_size(url)
                            if video_size == previous_processing_info['video_size']:
                                flavor_previously_analyzed = flavor
                                previous_processing_info['url'] = url_verified
                                video_size_mb = video_size/1024/1024
                                status_msg(
                                    f'flavor {flavor} of video {video_id} on mediaspace has the same size '
                                    f'({video_size_mb}MB) as in the last processing, so we skip the re-processing...',
                                    color='grey', sections=list(sections) + ['PROCESSING']
                                )
                                break
                        if flavor_previously_analyzed is None:
                            update_processing_time = False
            download_file = False
            for key_to_check in (
                    'video_token', 'audio_bit_rate', 'audio_codec_name', 'audio_duration',
                    'audio_sample_rate', 'video_bit_rate', 'video_codec_name', 'video_duration',
                    'video_resolution'
            ):
                if previous_processing_info[key_to_check] is None:
                    download_file = True
                    break
            if download_file:
                status_msg(
                    f'Information about the audio or/and video tracks are missing for {video_url}, collecting them...',
                    color='grey', sections=list(sections) + ['PROCESSING']
                )
                video_token, video_size, streams = download_url(
                    video_url, login_info, force=force, force_download=force_download, debug=debug
                )
                if video_token is None:
                    status_msg(
                        f'Failed to download the video {video_url}.',
                        color='red', sections=list(sections) + ['ERROR']
                    )
                    continue
                previous_processing_info['video_token'] = video_token
                previous_processing_info.update(get_video_information_from_streams(streams))
                if previous_processing_info['audio_fingerprint'] is None and \
                        previous_processing_info['audio_codec_name'] is not None:
                    audio_fingerprint = get_audio_fingerprint_of_video(
                        previous_processing_info['url'], login_info=login_info, force=force,
                        force_download=force_download, debug=debug
                    )
                    previous_processing_info['audio_fingerprint'] = audio_fingerprint
            if update_processing_time:
                video_duration_s = round(video_details["ms_duration"] / 1000)
                # platform, id and duration  are matching, we skip the reprocessing.
                if previous_processing_info['slides_detection_time']:
                    previous_processing_info['slides_detection_time'] = datetime.now()
                if previous_processing_info['slides_translation_time']:
                    previous_processing_info['slides_translation_time'] = datetime.now()
                if previous_processing_info['audio_transcription_time']:
                    previous_processing_info['audio_transcription_time'] = datetime.now()
                if previous_processing_info['slides_concept_extract_time']:
                    previous_processing_info['slides_concept_extract_time'] = datetime.now()
                if previous_processing_info['subtitles_concept_extract_time']:
                    previous_processing_info['subtitles_concept_extract_time'] = datetime.now()
                status_msg(
                    f'video {video_id} on {platform} did not change duration ({video_duration_s}s) '
                    f'so we just update the existing processing time(s).',
                    color='yellow', sections=list(sections) + ['SUCCESS']
                )
            elif video_details:
                for k, v in video_details.items():
                    if previous_processing_info.get(k, None) is None:
                        previous_processing_info[k] = v
            register_processed_video(piper_connection, platform, video_id, previous_processing_info)
            piper_connection.commit()


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
        ms_duration=ms_duration, video_size=octet_size, start_date=start_date, end_date=end_date
    )


def get_youtube_video_details(youtube_resource: GoogleResource, youtube_video_id):
    videos = youtube_resource.videos()
    channels = youtube_resource.channels()
    # see https://developers.google.com/youtube/v3/docs/videos/list for the list of properties
    video_request = videos.list(id=youtube_video_id, part='snippet,contentDetails,status')
    video_info_items = video_request.execute()['items']
    if len(video_info_items) == 0:
        status_msg(
            f'No information could be found about the YouTube video {youtube_video_id}',
            color='yellow', sections=['VIDEO', 'YOUTUBE', 'WARNING']
        )
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


def get_info_previous_video_processing(db, platform, video_id, video_url):
    """
    Get details about the previous analysis if it exists (video url is only used if video_id is None)

    :param db:
    :param platform:
    :param video_id:
    :param video_url:
    :return:
    """
    (
        previous_platform, previous_video_id, parent_video_id, video_token, audio_fingerprint,
        previous_url, thumbnail_url, video_creation_time, video_update_time, title, description, owner, creator,
        tags, ms_duration, video_size, audio_bit_rate, audio_codec_name, audio_duration, audio_sample_rate,
        video_bit_rate, video_codec_name, video_duration, video_resolution, start_date, end_date,
        slides_detected_language, audio_detected_language, slides_detection_time, slides_translation_time,
        audio_transcription_time, slides_concept_extract_time, subtitles_concept_extract_time
    ) = (None, ) * 33
    if platform is not None:
        condition = f'platform="{platform}" AND '
    else:
        condition = f'platform="other" AND '
    if video_id is not None:
        condition += f'videoId="{video_id}"'
    else:
        condition += f'videoUrl="{video_url}"'
    query = f'''SELECT
            platform,
            videoId,
            parentVideoId,
            videoToken,
            audioFingerprint,
            videoUrl,
            thumbnailUrl,
            videoCreationTime,
            videoUpdateTime,
            title,
            description,
            owner, 
            creator,
            tags, 
            msDuration,
            octetSize,
            audioBitRate,
            audioCodecName,
            audioDuration,
            audioSampleRate,
            videoBitRate,
            videoCodecName,
            videoDuration,
            videoResolution,
            startDate,
            endDate,
            slidesDetectedLanguage, 
            audioDetectedLanguage, 
            slidesDetectionTime, 
            slidesTranslationTime,
            audioTranscriptionTime,
            slidesConceptExtractionTime,
            subtitlesConceptExtractionTime
        FROM `gen_video`.`Videos` WHERE {condition};'''
    previous_analysis_info = execute_query(db, query)
    if previous_analysis_info:
        (
            previous_platform, previous_video_id, parent_video_id, video_token, audio_fingerprint,
            previous_url, thumbnail_url, video_creation_time, video_update_time, title, description, owner, creator,
            tags, ms_duration, video_size, audio_bit_rate, audio_codec_name, audio_duration, audio_sample_rate,
            video_bit_rate, video_codec_name, video_duration, video_resolution, start_date, end_date,
            slides_detected_language, audio_detected_language, slides_detection_time, slides_translation_time,
            audio_transcription_time, slides_concept_extract_time, subtitles_concept_extract_time
        ) = previous_analysis_info[-1]
    if previous_platform is None:
        previous_platform = platform
    if previous_video_id is None:
        previous_video_id = video_id
    if previous_url is None:
        previous_url = video_url
    return dict(
        platform=previous_platform, video_id=previous_video_id, parent_video_id=parent_video_id,
        video_token=video_token, audio_fingerprint=audio_fingerprint, url=previous_url, thumbnail_url=thumbnail_url,
        video_creation_time=video_creation_time, video_update_time=video_update_time, title=title,
        description=description, owner=owner, creator=creator, tags=tags, ms_duration=ms_duration,
        video_size=video_size, audio_bit_rate=audio_bit_rate, audio_codec_name=audio_codec_name,
        audio_duration=audio_duration, audio_sample_rate=audio_sample_rate, video_bit_rate=video_bit_rate,
        video_codec_name=video_codec_name, video_duration=video_duration, video_resolution=video_resolution,
        start_date=start_date, end_date=end_date, slides_detected_language=slides_detected_language,
        audio_detected_language=audio_detected_language, slides_detection_time=slides_detection_time,
        slides_translation_time=slides_translation_time, audio_transcription_time=audio_transcription_time,
        slides_concept_extract_time=slides_concept_extract_time,
        subtitles_concept_extract_time=subtitles_concept_extract_time
    )


def register_processed_video(db, platform, video_id, video_info, sections=('VIDEO', 'PROCESSING', 'REGISTER VIDEO')):
    if platform is None:
        platform = video_info.get('platform', 'other')
    if video_id is None:
        if platform == 'other':
            video_id = video_info['video_token']
            if video_id is None:
                raise ValueError(f'could not find a video_id for video at {video_info["video_url"]}.')
        else:
            raise ValueError(f'unexpected platform {platform} for video without video_id at {video_info["video_url"]}.')
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
            'slidesDetectionTime', 'slidesTranslationTime', 'audioTranscriptionTime',
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
            video_info['slides_detection_time'], video_info['slides_translation_time'],
            video_info['audio_transcription_time'],
            video_info['slides_concept_extract_time'], video_info['subtitles_concept_extract_time']
        )]
    )
    status_msg(
        f'Register video info for {video_id} on {platform}', color='green', sections=list(sections) + ['SUCCESS']
    )


if __name__ == '__main__':
    executable_name = sys.argv.pop(0)
    videos_url = sys.argv
    print(f'Get video information about {len(videos_url)} videos.')

    config_dir = realpath(join(dirname(__file__), '..',  'config'))
    piper_mysql_json_file = join(config_dir, "piper_db.json")
    graphai_json_file = join(config_dir, "graphai-api.json")
    get_video_info_on_rcp(videos_url, piper_mysql_json_file=piper_mysql_json_file, graph_api_json=graphai_json_file)

    print('Done')
