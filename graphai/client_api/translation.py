from graphai.client_api.utils import get_response
from time import sleep
from requests import get, post
from graphai.utils import status_msg
from typing import Union


def translate_text(
        text: Union[str, list], source_language, target_language, graph_ai_server='http://127.0.0.1:28800',
        sections=('GRAPHAI', 'TRANSLATE'), force=False, debug=False
):
    if text is None or len(text) == 0 or (len(text) == 1 and len(text[0]) == 1) or source_language == target_language:
        return text
    if isinstance(text, list):
        text_to_translate = []
        translated_line_to_original_mapping = {}
        for line_idx, line in enumerate(text):
            if isinstance(line, str):
                translated_line_to_original_mapping[len(text_to_translate)] = line_idx
                text_to_translate.append(line)
    else:
        text_to_translate = text
        translated_line_to_original_mapping = None
    response_translate = get_response(
        url=graph_ai_server + '/translation/translate',
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"text": text_to_translate, "source": source_language, "target": target_language, "force": force},
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
                status_msg(
                    f'text has already been translated in the past',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                status_msg(
                    f'translation of the text failed, task result was: {task_result}',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
            else:
                status_msg(
                    f'text has been translated',
                    color='green', sections=list(sections) + ['SUCCESS']
                )
            if task_result['text_too_large']:
                status_msg(
                    f'text was too large to be translated',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
            if isinstance(text, list) and isinstance(task_result['result'], list):
                translated_text_full = [None] * len(text)
                for tr_line_idx, translated_line in enumerate(task_result['result']):
                    original_line_idx = translated_line_to_original_mapping[tr_line_idx]
                    translated_text_full[original_line_idx] = translated_line
                return translated_text_full
            else:
                return task_result['result']
        elif translate_status == 'FAILURE':
            return None
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of translation for text: '
                + translate_status
            )
