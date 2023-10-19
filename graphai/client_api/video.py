from graphai.client_api.utils import get_response
from time import sleep
from requests import get, post
from graphai.utils import StatusMSG


def get_video_token(
        url_video, playlist=False, graph_ai_server='http://127.0.0.1:28800', sections=('GRAPHAI', 'DOWNLOAD VIDEO'),
        debug=False, force=False
):
    # retrieval of the video on graph-ai
    response_retrieve = get_response(
        url=graph_ai_server + '/video/retrieve_url',
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
            url=graph_ai_server + f'/video/retrieve_url/status/{task_id}',
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
            if not task_result['fresh']:
                StatusMSG(
                    f'{url_video} has already been retrieved in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'retrieving of {url_video} failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            else:
                StatusMSG(
                    f'{url_video} has been retrieved',
                    Color='green', Sections=list(sections) + ['SUCCESS']
                )
            return task_result['token']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of retrieve_url for {url_video}: ' + retrieve_status
            )


def fingerprint_video(
        video_token, force=False, graph_ai_server='http://127.0.0.1:28800', sections=('GRAPHAI', 'FINGERPRINT VIDEO'),
        debug=False
):
    response_fingerprint = get_response(
        url=graph_ai_server + '/video/calculate_fingerprint',
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
            url=graph_ai_server + f'/video/calculate_fingerprint/status/{task_id}',
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
            if not task_result['fresh']:
                StatusMSG(
                    f'{video_token} has already been fingerprinted in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'fingerprinting of {video_token} failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            else:
                StatusMSG(
                    f'{video_token} has been fingerprinted',
                    Color='green', Sections=list(sections) + ['SUCCESS']
                )
            return task_result['result']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of fingerprinting for {video_token}: '
                + fingerprint_status
            )


def extract_audio(
        video_token, force=False, graph_ai_server='http://127.0.0.1:28800', sections=('GRAPHAI', 'EXTRACT AUDIO'),
        debug=False
):
    response_extraction = get_response(
        url=graph_ai_server + '/video/extract_audio',
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
    while tries_extraction_status < 6000:
        tries_extraction_status += 1
        response_extraction_status = get_response(
            url=graph_ai_server + f'/video/extract_audio/status/{task_id}',
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
            if not task_result['fresh']:
                StatusMSG(
                    f'audio from {video_token} has already been extracted in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'extraction of the audio from {video_token} failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            else:
                StatusMSG(
                    f'audio has been extracted from {video_token}',
                    Color='green', Sections=list(sections) + ['SUCCESS']
                )
            return task_result['token']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of audio extraction for {video_token}: '
                + extraction_status
            )


def extract_slides(
        video_token, force=False, graph_ai_server='http://127.0.0.1:28800', sections=('GRAPHAI', 'EXTRACT SLIDES'),
        debug=False
):
    response_slides = get_response(
        url=graph_ai_server + '/video/detect_slides',
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
            url=graph_ai_server + f'/video/detect_slides/status/{task_id}',
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
            if not task_result['fresh']:
                StatusMSG(
                    f'slides from {video_token} has already been extracted in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'extraction of the slides from {video_token} failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            else:
                StatusMSG(
                    f'{len(task_result["slide_tokens"])} slides has been extracted from {video_token}',
                    Color='green', Sections=list(sections) + ['SUCCESS']
                )
            return task_result['slide_tokens']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of slide extraction for {video_token}: '
                + slides_status
            )

