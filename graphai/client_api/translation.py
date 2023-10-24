from graphai.client_api.utils import get_response
from time import sleep
from requests import get, post
from graphai.utils import StatusMSG
from typing import Union


def translate_text(
        text: Union[str, list], source_language, target_language, graph_ai_server='http://127.0.0.1:28800',
        sections=('GRAPHAI', 'TRANSLATE'), force=False, debug=False
):
    if text is None or len(text) == 0 or source_language == target_language:
        return text
    response_translate = get_response(
        url=graph_ai_server + '/translation/translate',
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"text": text, "source": source_language, "target": target_language, "force": force},
        sections=sections,
        debug=debug
    )
    if response_translate is None:
        return None
    task_id = response_translate.json()['task_id']
    # wait for the translation to be completed
    tries_translate_status = 0
    while tries_translate_status < 6000:
        tries_translate_status += 1
        response_translate_status = get_response(
            url=graph_ai_server + f'/translation/translate/status/{task_id}',
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections,
            debug=debug
        )
        if response_translate_status is None:
            return None
        response_translate_status_json = response_translate_status.json()
        translate_status = response_translate_status_json['task_status']
        if translate_status in ['PENDING', 'STARTED'] or response_translate_status_json['task_result'] is None:
            sleep(1)
        elif translate_status == 'SUCCESS':
            task_result = response_translate_status_json['task_result']
            if not task_result['fresh']:
                StatusMSG(
                    f'text has already been translated in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'translation of the text failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            else:
                StatusMSG(
                    f'text has been translated',
                    Color='green', Sections=list(sections) + ['SUCCESS']
                )
            if task_result['text_too_large']:
                StatusMSG(
                    f'text was too large to be translated',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            return task_result['result']
        elif translate_status == 'FAILURE':
            return None
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of translation for text: '
                + translate_status
            )
