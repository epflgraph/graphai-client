from typing import Optional
from graphai_client.client_api.utils import status_msg, call_async_endpoint
from graphai_client.utils import get_google_api_credentials


def extract_text_from_slide(
        slide_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'OCR'), debug=False, quiet=False,
        max_tries=5, max_processing_time_s=600, google_api_token: str = None,
) -> Optional[dict[str, str]]:
    """
    extract text (using google OCR) from a single slide

    :param slide_token: slide token, typically obtained from graphai_client.client_api.video.extract_slides().
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the slide extraction forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param quiet: disable success status messages.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to perform the text extraction.
    :param google_api_token: the google API token charged for the use of the OCR.
    :return: a dictionary with the text extracted as value of the 'text' key and the detected language as value of the
        'language' key if successful, None otherwise.
    """
    if google_api_token is None:
        google_api_token = get_google_api_credentials(service_name='vision').get('developerKey', None)
    if google_api_token is None:
        raise RuntimeError('please provide a google_api_token enabled for the vision service')
    task_result = call_async_endpoint(
        endpoint='/image/extract_text',
        json={"token": slide_token, "method": "google", "force": force, "google_api_token": google_api_token},
        login_info=login_info,
        token=slide_token,
        output_type='text',
        result_key=None,
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug,
        quiet=quiet
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
        max_tries=5, max_processing_time_s=120, quiet=False
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
    :param quiet: if True the log messages will not be displayed.
    :return: the fingerprint of the slide if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/image/calculate_fingerprint',
        json={"token": slide_token, "force": force},
        login_info=login_info,
        token=slide_token,
        output_type='fingerprint',
        result_key='result',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug,
        quiet=quiet
    )
    if task_result is None:
        return None
    return task_result['result']
