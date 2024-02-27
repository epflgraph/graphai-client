from typing import Optional
from graphai_client.client_api.utils import call_async_endpoint


def get_video_token(
        url_video: str, login_info: dict, playlist=False, sections=('GRAPHAI', 'DOWNLOAD VIDEO'),
        debug=False, force=False, n_try=6000, delay_retry=1
) -> Optional[str]:
    """
    Download a video and get a token.

    :param url_video: url of the video to download.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param playlist: see the API documentation.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param force: Should the cache be bypassed and the download forced.
    :param n_try: the number of tries before giving up.
    :param delay_retry: the time to wait between tries.
    :return: the video token if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/video/retrieve_url',
        json={"url": url_video, "playlist": playlist, "force": force},
        login_info=login_info,
        token=url_video,
        output_type='file',
        n_try=n_try,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    return task_result['token']


def fingerprint_video(
        video_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'FINGERPRINT VIDEO'), debug=False,
        n_try=6000, delay_retry=1
) -> Optional[str]:
    """
    Get the fingerprint of a video.

    :param video_token: video token, typically returned by get_video_token()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the fingerprint forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param n_try: the number of tries before giving up.
    :param delay_retry: the time to wait between tries.
    :return: the fingerprint of the video if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/video/calculate_fingerprint',
        json={"token": video_token, "force": force},
        login_info=login_info,
        token=video_token,
        output_type='fingerprint',
        n_try=n_try,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    return task_result['result']


def extract_audio(
        video_token: str, login_info: dict, force=False, force_non_self=True, sections=('GRAPHAI', 'EXTRACT AUDIO'),
        debug=False, n_try=300, delay_retry=1
):
    """
    extract the audio from a video and return the audio token.

    :param video_token: video token, typically returned by get_video_token()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the audio extraction forced.
    :param force_non_self: see the API documentation.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param n_try: the number of tries before giving up.
    :param delay_retry: the time to wait between tries.
    :return: the audio token if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/video/extract_audio',
        json={"token": video_token, "force_non_self": force_non_self, "force": force},
        login_info=login_info,
        token=video_token,
        output_type='audio',
        n_try=n_try,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    return task_result['token']


def extract_slides(
        video_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'EXTRACT SLIDES'), debug=False,
        n_try=6000, delay_retry=1
) -> Optional[dict]:
    """
    Extract slides from a video. Slides are defined as a times in a video where there is a significant visual change.

    :param video_token: video token, typically returned by get_video_token()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the slides extraction forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param n_try: the number of tries before giving up.
    :param delay_retry: the time to wait between tries.
    :return: A dictionary with slide number as string for keys and a dictionary with slide token and timestamp as values
        if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/video/detect_slides',
        json={"token": video_token, "force_non_self": False, "force": force},
        login_info=login_info,
        token=video_token,
        output_type='slide',
        n_try=n_try,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
