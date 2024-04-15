from typing import Optional
from graphai_client.client_api.utils import call_async_endpoint, status_msg


def get_video_token(
        url_video: str, login_info: dict, playlist=False, sections=('GRAPHAI', 'DOWNLOAD VIDEO'),
        debug=False, force=False, results_needed=(), max_tries=5, max_processing_time_s=900
) -> Optional[str]:
    """
    Download a video and get a token.

    :param url_video: url of the video to download.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param playlist: see the API documentation.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param force: Should the cache be bypassed and the download forced.
    :param results_needed: list of operations which should be available in cache otherwise the audio extraction is
        extracted according to cache results.
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
            f'Invalid token status while retrieving video {url_video}',
            color='yellow', sections=list(sections) + ['WARNING']
        )
    elif not token_status.get("active", None):
        if task_result.get('fresh', None):
            raise RuntimeError(f'Missing downloaded file from {url_video} while fresh')
        if force:
            raise RuntimeError(f'Missing downloaded file from {url_video} while forced')
        force_download = False
        for result in results_needed:
            cached_jobs = token_status.get('cached', None) or []
            if result not in cached_jobs:
                force_download = True
                break
        if force_download:
            status_msg(
                f'Missing downloaded file from {url_video}, force downloading...',
                sections=list(sections) + ['WARNING'], color='yellow'
            )
            return get_video_token(
                url_video=url_video, login_info=login_info, playlist=playlist, sections=sections, debug=debug,
                force=True, results_needed=results_needed, max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
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
        video_token: str, login_info: dict, recalculate_cached=False, force=False, results_needed=(),
        sections=('GRAPHAI', 'EXTRACT AUDIO'), debug=False, max_tries=5, max_processing_time_s=300
) -> Optional[str]:
    """
    extract the audio from a video and return the audio token.

    :param video_token: video token, typically returned by get_video_token()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param recalculate_cached: extract audio based on the cached results.
    :param force: Should the cache be bypassed and the audio extraction forced.
    :param results_needed: list of operations which should be available in cache otherwise the audio extraction is
        extracted according to cache results.
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
            raise RuntimeError(f'Missing file for fresh audio extracted from {video_token}')
        if force:
            raise RuntimeError(f'Missing file for audio extracted from {video_token} while forced')
        if recalculate_cached:
            raise RuntimeError(f'Missing file for audio extracted from {video_token} while recalculated')
        force_extraction = False
        for result in results_needed:
            cached_jobs = token_status.get('cached', None) or []
            if result not in cached_jobs:
                force_extraction = True
                break
        if force_extraction:
            status_msg(
                f'Missing file for audio extracted from {video_token}, extracting audio according to the cache...',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            return extract_audio(
                video_token, login_info, recalculate_cached=True, force=force,
                results_needed=results_needed, sections=sections, debug=debug, max_tries=max_tries,
                max_processing_time_s=max_processing_time_s
            )
    return task_result['token']


def extract_slides(
        video_token: str, login_info: dict, recalculate_cached=False, force=False, results_needed=(),
        max_tries=5, max_processing_time_s=6000, sections=('GRAPHAI', 'EXTRACT SLIDES'), debug=False
) -> Optional[dict]:
    """
    Extract slides from a video. Slides are defined as a times in a video where there is a significant visual change.

    :param video_token: video token, typically returned by get_video_token()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param recalculate_cached: extract slides based on the cached results.
    :param force: Should the cache be bypassed and the slides extraction forced.
    :param results_needed: list of operations which should be available in cache for all slides otherwise the
        slides extraction is performed according to cache results.
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
            for result in results_needed:
                cached_jobs = token_status.get('cached', None) or []
                if result not in cached_jobs:
                    num_missing_slides += 1
                    break
    if num_missing_slides > 0:
        if task_result.get('fresh', None):
            raise RuntimeError(
                f'Missing {num_missing_slides}/{len(task_result["slide_tokens"])} slide files'
                f' for fresh audio extracted from {video_token}'
            )
        if force:
            raise RuntimeError(
                f'Missing {num_missing_slides}/{len(task_result["slide_tokens"])} slide files'
                f' for audio extracted from {video_token} while forced'
            )
        if recalculate_cached:
            raise RuntimeError(
                f'Missing {num_missing_slides}/{len(task_result["slide_tokens"])} slide files'
                f' for audio extracted from {video_token} while recalculated'
            )
        status_msg(
            f'Missing {num_missing_slides}/{len(task_result["slide_tokens"])} slide files for '
            f'video {video_token}, extracting slides according to the cache...',
            sections=list(sections) + ['WARNING'], color='yellow'
        )
        return extract_slides(
            video_token=video_token, login_info=login_info, recalculate_cached=True, force=force,
            max_tries=max_tries, max_processing_time_s=max_processing_time_s, sections=sections, debug=debug
        )
    return task_result['slide_tokens']
