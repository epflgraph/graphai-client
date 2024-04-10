from typing import Optional
from graphai_client.client_api.utils import call_async_endpoint, status_msg


def get_video_token(
        url_video: str, login_info: dict, playlist=False, sections=('GRAPHAI', 'DOWNLOAD VIDEO'),
        debug=False, force=False, max_tries=5, max_processing_time_s=900
) -> Optional[str]:
    """
    Download a video and get a token.

    :param url_video: url of the video to download.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param playlist: see the API documentation.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param force: Should the cache be bypassed and the download forced.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to download the video.
    :return: the video token if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/video/retrieve_url',
        json={"url": url_video, "playlist": playlist, "force": force},
        login_info=login_info,
        token=url_video,
        output_type='file',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    if not task_result['token_status']['active']:
        status_msg('Missing video file in cache, downloading...', sections=list(sections) + ['WARNING'], color='yellow')
        return get_video_token(
            url_video=url_video, login_info=login_info, playlist=playlist, max_tries=max_tries,
            max_processing_time_s=max_processing_time_s, sections=sections, debug=debug, force=True
        )
    return task_result['token']


def fingerprint_video(
        video_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'FINGERPRINT VIDEO'), debug=False,
        max_tries=5, max_processing_time_s=900
) -> Optional[str]:
    """
    Get the fingerprint of a video.

    :param video_token: video token, typically returned by get_video_token()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the fingerprint forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to fingerprint the video.
    :return: the fingerprint of the video if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/video/calculate_fingerprint',
        json={"token": video_token, "force": force},
        login_info=login_info,
        token=video_token,
        output_type='fingerprint',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    return task_result['result']


def extract_audio(
        video_token: str, login_info: dict, recalculate_cached=False, force=False,
        sections=('GRAPHAI', 'EXTRACT AUDIO'), debug=False, max_tries=5, max_processing_time_s=300
):
    """
    extract the audio from a video and return the audio token.

    :param video_token: video token, typically returned by get_video_token()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param recalculate_cached: extract audio based on the cached results.
    :param force: Should the cache be bypassed and the audio extraction forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to fingerprint the video.
    :return: the audio token if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/video/extract_audio',
        json={"token": video_token, "recalculate_cached": recalculate_cached, "force": force},
        login_info=login_info,
        token=video_token,
        output_type='audio',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    return task_result['token']


def extract_slides(
        video_token: str, login_info: dict, recalculate_cached=False, force=False,
        max_tries=5, max_processing_time_s=6000, sections=('GRAPHAI', 'EXTRACT SLIDES'), debug=False
) -> Optional[dict]:
    """
    Extract slides from a video. Slides are defined as a times in a video where there is a significant visual change.

    :param video_token: video token, typically returned by get_video_token()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param recalculate_cached: extract slides based on the cached results.
    :param force: Should the cache be bypassed and the slides extraction forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to extract slides from the video.
    :return: A dictionary with slide number as a string for keys and a dictionary with slide token and timestamp as
        values if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/video/detect_slides',
        json={"token": video_token, "recalculate_cached": recalculate_cached, "force": force},
        login_info=login_info,
        token=video_token,
        output_type='slides',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    if not force and not recalculate_cached and not task_result['fresh']:
        force_recalculate_cached = False
        for slide_index, slide_dict in task_result['slide_tokens'].items():
            if not slide_dict['token_status']['active']:
                force_recalculate_cached = True
        if force_recalculate_cached:
            status_msg(
                'Missing slide files in cache, extracting them from the videos...',
                sections=list(sections) + ['WARNING'], color='yellow'
            )
            return extract_slides(
                video_token=video_token, login_info=login_info, recalculate_cached=True, force=False,
                max_tries=max_tries, max_processing_time_s=max_processing_time_s, sections=sections, debug=debug
            )
    return task_result['slide_tokens']
