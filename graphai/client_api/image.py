from graphai.client_api.utils import get_response, task_result_is_ok
from time import sleep
from requests import get, post
from graphai.utils import status_msg


def extract_text_from_slide(
        slide_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'OCR'), debug=False
):
    # extract text (using google OCR) from a single slide
    response_text = get_response(
        url='/image/extract_text',
        login_info=login_info,
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"token": slide_token, "method": "google", "force": force},
        sections=sections,
        debug=debug
    )
    if response_text is None:
        return None
    task_id = response_text.json()['task_id']
    # wait for the detection of slides to be completed
    tries_text_status = 0
    while tries_text_status < 600:
        tries_text_status += 1
        response_text_status = get_response(
            url=f'/image/extract_text/status/{task_id}',
            login_info=login_info,
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections,
            debug=debug
        )
        if response_text_status is None:
            return None
        response_text_status_json = response_text_status.json()
        text_status = response_text_status_json['task_status']
        if text_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif text_status == 'SUCCESS':
            task_result = response_text_status_json['task_result']
            if not task_result_is_ok(task_result, token=slide_token, input_type='text', sections=sections):
                sleep(1)
                continue
            for result in task_result['result']:
                # we use document text detection which should perform better with coherent documents
                if result['method'] == 'ocr_google_1_token' or result['method'] == 'ocr_google_1_results':
                    return {'text': result['text'], 'language': task_result['language']}
            status_msg(
                f'document text detection result not found',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            return {'text': task_result['result'][0]['text'], 'language': task_result['language']}
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of text extraction for {slide_token}: '
                + text_status
            )
