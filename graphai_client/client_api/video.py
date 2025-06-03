from typing import Optional, Tuple
from requests import post, Session
from graphai_client.client_api.utils import call_async_endpoint, status_msg, _get_response


def get_video_token(
        url_video: str, login_info: dict, playlist=False, sections=('GRAPHAI', 'DOWNLOAD VIDEO'),
        debug=False, force=False, max_tries=5, max_processing_time_s=900
) -> Tuple[Optional[str], Optional[int], Optional[list]]:
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
    :return: A tuple with the video token, the video size in octet and the stream content of the video if successful,
        (None, None, None) otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/video/retrieve_url',
        json={"url": url_video, "playlist": playlist, "force": force},
        login_info=login_info,
        token=url_video,
        output_type='file',
        result_key='token',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        status_msg(
            f'Failed to download the video {url_video}',
            color='red', sections=list(sections) + ['WARNING']
        )
        return None, None, None
    token_size = task_result.get('token_size', 0)
    streams = task_result.get('token_status', {}).get('streams', None)
    if token_size == 0:
        status_msg(
            f'Empty video got for {url_video}',
            color='yellow', sections=list(sections) + ['WARNING']
        )
        return None, None, None
    token_status = task_result.get('token_status', None)
    if not token_status:
        status_msg(
            f'Invalid token status while retrieving video {url_video}',
            color='yellow', sections=list(sections) + ['WARNING']
        )
    elif not token_status.get("active", None):
        if task_result.get('fresh', None):
            raise RuntimeError(f'Missing downloaded file from {url_video} while fresh')
        if force:
            raise RuntimeError(f'Missing downloaded file from {url_video} while forced')
        status_msg(
            f'Missing downloaded file from {url_video}, force downloading...',
            sections=list(sections) + ['INFO'], color='grey'
        )
        return get_video_token(
            url_video=url_video, login_info=login_info, playlist=playlist, sections=sections, debug=debug,
            force=True, max_tries=max_tries, max_processing_time_s=max_processing_time_s,
        )
    return task_result['token'], token_size, streams


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
        result_key='result',
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
) -> Optional[str]:
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
        result_key='token',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    token_status = task_result.get('token_status', None)
    if not token_status:
        status_msg(
            f'Invalid token status while extracting audio from video {video_token}',
            color='yellow', sections=list(sections) + ['WARNING']
        )
    elif not token_status.get("active", None):
        if task_result.get('fresh', None):
            raise RuntimeError(f'Missing file for fresh audio extracted from {video_token} while fresh')
        if force:
            raise RuntimeError(f'Missing file for audio extracted from {video_token} while forced')
        if recalculate_cached:
            raise RuntimeError(f'Missing file for audio extracted from {video_token} while recalculated')
        status_msg(
            f'Missing file for audio extracted from {video_token}, extracting audio according to the cache...',
            color='grey', sections=list(sections) + ['INFO']
        )
        return extract_audio(
            video_token, login_info, recalculate_cached=True, force=force, sections=sections, debug=debug,
            max_tries=max_tries, max_processing_time_s=max_processing_time_s
        )
    return task_result['token']


def extract_slides(
        video_token: str, login_info: dict, recalculate_cached=False, force=False,
        max_tries=5, max_processing_time_s=6000, sections=('GRAPHAI', 'EXTRACT SLIDES'), debug=False,
        hash_thresh=0.95, multiplier=5, default_threshold=0.05, include_first=True, include_last=True
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
    :param hash_thresh: Maximum hash similarity at which two images are not considered identical
    :param multiplier: Multiplier for noise level calculation
    :param default_threshold: Default noise level when calculation does not yield a result
    :param include_first: Whether to force-include the first frame as a slide
    :param include_last: Whether to force-include the last frame as a slide
    :return: A dictionary with slide number as a string for keys and a dictionary with slide token and timestamp as
        values if successful, None otherwise.
    """
    parameters = dict(
        hash_thresh=hash_thresh,
        multiplier=multiplier,
        default_threshold=default_threshold,
        include_first=include_first,
        include_last=include_last
    )
    task_result = call_async_endpoint(
        endpoint='/video/detect_slides',
        json={"token": video_token, "recalculate_cached": recalculate_cached, "force": force, "parameters": parameters},
        login_info=login_info,
        token=video_token,
        output_type='slides',
        result_key='slide_tokens',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    num_missing_slides = 0
    for slide_index, slide_dict in task_result['slide_tokens'].items():
        token_status = slide_dict.get("token_status", None)
        if not token_status:
            status_msg(
                f'Invalid token status for slide {slide_index} of video {video_token}',
                color='yellow', sections=list(sections) + ['WARNING']
            )
        elif not token_status.get("active", None):
            num_missing_slides += 1
    if num_missing_slides > 0:
        if task_result.get('fresh', None):
            status_msg(
                f'Missing {num_missing_slides}/{len(task_result["slide_tokens"])} slide files from {video_token} '
                f'while fresh, forcing extraction...', sections=list(sections) + ['WARNING'], color='yellow'
            )
            return extract_slides(
                video_token=video_token, login_info=login_info, recalculate_cached=False, force=True,
                max_tries=max_tries, max_processing_time_s=max_processing_time_s, sections=sections, debug=debug
            )
        if force:
            raise RuntimeError(
                f'Missing {num_missing_slides}/{len(task_result["slide_tokens"])} slide files from {video_token} '
                f'while forced'
            )
        if recalculate_cached:
            raise RuntimeError(
                f'Missing {num_missing_slides}/{len(task_result["slide_tokens"])} slide files from {video_token} '
                f'while recalculated'
            )
        status_msg(
            f'Missing {num_missing_slides}/{len(task_result["slide_tokens"])} slide files from {video_token}, '
            f'extracting slides according to the cache...',
            sections=list(sections) + ['INFO'], color='grey'
        )
        return extract_slides(
            video_token=video_token, login_info=login_info, recalculate_cached=True, force=force,
            max_tries=max_tries, max_processing_time_s=max_processing_time_s, sections=sections, debug=debug
        )
    return task_result['slide_tokens']


def download_file(
        token: str, file_path: str, login_info: dict, max_tries=5, timeout=60, sections=('GRAPHAI', 'DOWNLOAD FILE'),
        delay_retry=60, session: Optional[Session] = None, debug=False
) -> Optional[str]:
    """
    Download the resource identified by its token (video file, audio or slide) and save it in the specified file_path.

    :param token: token of the resource to download.
    :param file_path: where to save the downloaded file.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json)
    :param max_tries: number of trials to perform in case of errors before giving up.
    :param timeout: timeout for the request to complete
    :param sections: sections to use in the status messages.
    :param delay_retry:  the time to wait between tries.
    :param session: optional requests.Session object.
    :param debug: if True additional information about each connection to the API is displayed.
    :return: the path to the downloaded resource if successful, None otherwise.
    """
    if session is None:
        request_func = post
    else:
        request_func = session.post
    url = login_info['host'] + '/video/get_file'
    json = {'token': token}
    response = _get_response(
        url, login_info, request_func=request_func, json=json, max_tries=max_tries, sections=sections, debug=debug,
        delay_retry=delay_retry, timeout=timeout
    )
    if response is None:
        return None
    with open(file_path, 'wb') as fid:
        fid.write(response.content)
    return file_path
