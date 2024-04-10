from typing import Optional, Literal
from graphai_client.client_api.utils import call_async_endpoint
from graphai_client.utils import status_msg


def extract_text_from_slide(
        slide_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'OCR'), debug=False,
        max_tries=5, max_processing_time_s=600
) -> Optional[dict[str, str]]:
    """
    extract text (using google OCR) from a single slide

    :param slide_token: slide token, typically obtained from graphai_client.client_api.video.extract_slides().
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the slide extraction forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to perform the text extraction.
    :return: a dictionary with the text extracted as value of the 'text' key and the detected language as value of the
        'language' key if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/image/extract_text',
        json={"token": slide_token, "method": "google", "force": force},
        login_info=login_info,
        token=slide_token,
        output_type='text',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    for result in task_result['result']:
        # we use document text detection which should perform better with coherent documents
        if result['method'] == 'ocr_google_1_token' or result['method'] == 'ocr_google_1_results':
            return {'text': result['text'], 'language': task_result['language']}
    status_msg(
        f'document text detection result not found',
        color='yellow', sections=list(sections) + ['WARNING']
    )
    return {'text': task_result['result'][0]['text'], 'language': task_result['language']}


def calculate_fingerprint(
        slide_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'SLIDE FINGERPRINT'), debug=False,
        max_tries=5, max_processing_time_s=20
):
    """
    Calculate the fingerprint of a single slide

    :param slide_token: slide token, typically obtained from graphai_client.client_api.video.extract_slides().
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the slide extraction forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to perform the text extraction.
    :return: the fingerprint of the slide if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/image/calculate_fingerprint',
        json={"token": slide_token, "force": force},
        login_info=login_info,
        token=slide_token,
        output_type='fingerprint',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    return task_result['result']
