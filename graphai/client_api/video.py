from graphai.client_api.utils import get_response, task_result_is_ok
from time import sleep
from requests import get, post


def get_video_token(
        url_video: str, login_info: dict, playlist=False, sections=('GRAPHAI', 'DOWNLOAD VIDEO'),
        debug=False, force=False
):
    # retrieval of the video on graph-ai
    response_retrieve = get_response(
        url='/video/retrieve_url',
        login_info=login_info,
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"url": url_video, "playlist": playlist, "force": force},
        sections=sections,
        debug=debug
    )
    if response_retrieve is None:
        return None
    task_id = response_retrieve.json()['task_id']
    # wait for the retrieval to be completed
    tries_retrieve_status = 0
    while tries_retrieve_status < 6000:
        tries_retrieve_status += 1
        response_retrieve_status = get_response(
            url=f'/video/retrieve_url/status/{task_id}',
            login_info=login_info,
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections,
            debug=debug
        )
        if response_retrieve_status is None:
            return None
        response_retrieve_status_json = response_retrieve_status.json()
        retrieve_status = response_retrieve_status_json['task_status']
        if retrieve_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif retrieve_status == 'SUCCESS':
            task_result = response_retrieve_status_json['task_result']
            if not task_result_is_ok(task_result, token=url_video, input_type='file', sections=sections):
                sleep(1)
                continue
            return task_result['token']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of retrieve_url for {url_video}: ' + retrieve_status
            )


def fingerprint_video(
        video_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'FINGERPRINT VIDEO'), debug=False
):
    response_fingerprint = get_response(
        url='/video/calculate_fingerprint',
        login_info=login_info,
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"token": video_token, "force": force},
        sections=sections,
        debug=debug
    )
    if response_fingerprint is None:
        return None
    task_id = response_fingerprint.json()['task_id']
    # wait for the fingerprinting to be completed
    tries_fingerprint_status = 0
    while tries_fingerprint_status < 6000:
        tries_fingerprint_status += 1
        response_fingerprint_status = get_response(
            url=f'/video/calculate_fingerprint/status/{task_id}',
            login_info=login_info,
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections,
            debug=debug
        )
        if response_fingerprint_status is None:
            return None
        response_fingerprint_status_json = response_fingerprint_status.json()
        fingerprint_status = response_fingerprint_status_json['task_status']
        if fingerprint_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif fingerprint_status == 'SUCCESS':
            task_result = response_fingerprint_status_json['task_result']
            if not task_result_is_ok(task_result, token=video_token, input_type='fingerprint', sections=sections):
                sleep(1)
                continue
            return task_result['result']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of fingerprinting for {video_token}: '
                + fingerprint_status
            )


def extract_audio(
        video_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'EXTRACT AUDIO'), debug=False
):
    response_extraction = get_response(
        url='/video/extract_audio',
        login_info=login_info,
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"token": video_token, "force_non_self": True, "force": force},
        sections=sections,
        debug=debug
    )
    if response_extraction is None:
        return None
    task_id = response_extraction.json()['task_id']
    # wait for the extraction to be completed
    tries_extraction_status = 0
    while tries_extraction_status < 300:
        tries_extraction_status += 1
        response_extraction_status = get_response(
            url=f'/video/extract_audio/status/{task_id}',
            login_info=login_info,
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections,
            debug=debug
        )
        if response_extraction_status is None:
            return None
        response_extraction_status_json = response_extraction_status.json()
        extraction_status = response_extraction_status_json['task_status']
        if extraction_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif extraction_status == 'SUCCESS':
            task_result = response_extraction_status_json['task_result']
            if not task_result_is_ok(task_result, token=video_token, input_type='audio', sections=sections):
                sleep(1)
                continue
            return task_result['token']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of audio extraction for {video_token}: '
                + extraction_status
            )


def extract_slides(
        video_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'EXTRACT SLIDES'), debug=False
) -> dict:
    """
    :return: dictionary with slide number as string for keys and a dictionary with slide token and timestamp as values.
    """
    response_slides = get_response(
        url='/video/detect_slides',
        login_info=login_info,
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"token": video_token, "force_non_self": False, "force": force},
        sections=sections,
        debug=debug
    )
    if response_slides is None:
        return None
    task_id = response_slides.json()['task_id']
    # wait for the detection of slides to be completed
    tries_slides_status = 0
    while tries_slides_status < 6000:
        tries_slides_status += 1
        response_slides_status = get_response(
            url=f'/video/detect_slides/status/{task_id}',
            login_info=login_info,
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections,
            debug=debug
        )
        if response_slides_status is None:
            return None
        response_slides_status_json = response_slides_status.json()
        slides_status = response_slides_status_json['task_status']
        if slides_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif slides_status == 'SUCCESS':
            task_result = response_slides_status_json['task_result']
            if not task_result_is_ok(task_result, token=video_token, input_type='slide', sections=sections):
                sleep(1)
                continue
            return task_result['slide_tokens']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of slide extraction for {video_token}: '
                + slides_status
            )

