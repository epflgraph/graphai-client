from time import sleep
from requests import get, post
from typing import Union
from graphai.utils import status_msg
from graphai.client_api.utils import get_response, task_result_is_ok


def translate_text(
        text: Union[str, list], source_language, target_language, graph_ai_server='http://127.0.0.1:28800',
        sections=('GRAPHAI', 'TRANSLATE'), force=False, debug=False
):
    if text is None or len(text) == 0 or \
            (len(text) == 1 and (text[0] is None or len(text[0]) == 0)) or\
            source_language == target_language:
        return text
    # use english as an intermediary if needed
    if source_language not in ('en', 'fr') and target_language != 'en':
        translated_text_en = translate_text(
            text, source_language, 'en', graph_ai_server=graph_ai_server,
            sections=sections, force=force, debug=debug
        )
        if translated_text_en is None:
            status_msg(
                f'failed to translate "{text}" from {source_language} into en',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            return None
        return translate_text(
            translated_text_en, 'en', target_language, graph_ai_server=graph_ai_server,
            sections=sections, force=force, debug=debug
        )
    # get rid of None in list input
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
    while tries_translate_status < 3000:
        tries_translate_status += 1
        try:
            response_translate_status = get_response(
                url=graph_ai_server + f'/translation/translate/status/{task_id}',
                request_func=get,
                headers={'Content-Type': 'application/json'},
                sections=sections,
                debug=debug
            )
        except RuntimeError as e:
            status_msg(
                f'Translation from {source_language} to {target_language} caused an exception, '
                f'the text to translate was:\n{text_to_translate}',
                color='red', sections=list(sections) + ['ERROR']
            )
            raise e
        if response_translate_status is None:
            return None
        response_translate_status_json = response_translate_status.json()
        translate_status = response_translate_status_json['task_status']
        if translate_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif translate_status == 'SUCCESS':
            task_result = response_translate_status_json['task_result']
            if not task_result_is_ok(
                    task_result, token=source_language + ' text', input_type='translation', sections=sections
            ):
                status_msg(
                    f'text was:\n{text_to_translate}',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
                sleep(1)
                continue
            if task_result['text_too_large']:
                status_msg(
                    f'text was too large to be translated',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
                return None
            else:
                status_msg(
                    f'text has been translated',
                    color='green', sections=list(sections) + ['SUCCESS']
                )
            # put back None in the output so the number of element is the same as in the input
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
    status_msg(
        f'timeout while trying to translate the following text:\n{text_to_translate}',
        color='yellow', sections=list(sections) + ['WARNING']
    )
    return None
