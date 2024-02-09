from time import sleep
from requests import get, post
from typing import Union
from graphai.utils import status_msg
from graphai.client_api.utils import get_response, task_result_is_ok, split_text


def translate_text(
        text: Union[str, list], source_language, target_language, graph_ai_server='http://127.0.0.1:28800',
        sections=('GRAPHAI', 'TRANSLATE'), force=False, debug=False, max_text_length=None, max_text_list_length=50000
):
    if source_language == target_language:
        return text
    # check for empty text
    if isinstance(text, str) and not text:
        return text
    else:  # check for list of empty text
        is_empty = True
        for line in text:
            if line and line.strip():
                is_empty = False
                break
        if is_empty:
            return text
    # use english as an intermediary if needed
    if source_language not in ('en', 'fr') and target_language != 'en':
        translated_text_en = translate_text(
            text, source_language, 'en', graph_ai_server=graph_ai_server, sections=sections, force=force, debug=debug,
            max_text_length=max_text_length, max_text_list_length=max_text_list_length
        )
        if translated_text_en is None:
            status_msg(
                f'failed to translate "{text}" from {source_language} into en',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            return None
        return translate_text(
            translated_text_en, 'en', target_language, graph_ai_server=graph_ai_server, sections=sections, force=force,
            debug=debug, max_text_length=max_text_length, max_text_list_length=max_text_list_length
        )
    # split in smaller lists if the text is too large
    if isinstance(text, list):
        lengths_text = [len(t) if t is not None else 0 for t in text]
        if sum(lengths_text) > max_text_list_length:
            idx_start = 0
            sum_length = 0
            n_text_elems = len(text)
            translated_text_full = [None]*n_text_elems
            for idx_end in range(n_text_elems):
                sum_length += lengths_text[idx_end]
                # we reached the end
                if idx_end + 1 == n_text_elems:
                    status_msg(
                        f'get part of the text (from {idx_start} to the {n_text_elems}) as the full list is too long',
                        color='grey', sections=list(sections) + ['PROCESSING']
                    )
                    translated_text_full[idx_start:] = translate_text(
                        text[idx_start:], source_language, target_language, graph_ai_server=graph_ai_server,
                        sections=sections, force=force, debug=debug, max_text_length=max_text_length,
                        max_text_list_length=max_text_list_length
                    )
                # one element is already too large
                elif sum_length > max_text_list_length:
                    status_msg(
                        f'get part of the text (at {idx_start}) as the full list is too long',
                        color='grey', sections=list(sections) + ['PROCESSING']
                    )
                    translated_text_full[idx_start] = translate_text(
                        text[idx_start], source_language, target_language, graph_ai_server=graph_ai_server,
                        sections=sections, force=force, debug=debug, max_text_length=max_text_length,
                        max_text_list_length=max_text_list_length
                    )
                    idx_start += 1
                    sum_length = 0
                # with the next element it is too large or we reached the end
                elif sum_length + lengths_text[idx_end + 1] > max_text_list_length:
                    status_msg(
                        f'get part of the text (from {idx_start} to {idx_end}) as the full list is too long',
                        color='grey', sections=list(sections) + ['PROCESSING']
                    )
                    translated_text_full[idx_start:idx_end+1] = translate_text(
                        text[idx_start:idx_end+1], source_language, target_language, graph_ai_server=graph_ai_server,
                        sections=sections, force=force, debug=debug, max_text_length=max_text_length,
                        max_text_list_length=max_text_list_length
                    )
                    idx_start = idx_end + 1
                    sum_length = 0
            return translated_text_full
    # get rid of None in list input
    if isinstance(text, list):
        text_to_translate = []
        translated_line_to_original_mapping = {}
        for line_idx, line in enumerate(text):
            if isinstance(line, str):
                if max_text_length and len(line) > max_text_length:
                    split_line = split_text(line, max_text_length)
                    for line_portion in split_line:
                        translated_line_to_original_mapping[len(text_to_translate)] = line_idx
                        text_to_translate.append(line_portion)
                else:
                    translated_line_to_original_mapping[len(text_to_translate)] = line_idx
                    text_to_translate.append(line)
    else:
        if max_text_length and len(text) > max_text_length:
            translated_line_to_original_mapping = {}
            text_to_translate = []
            text_portions = split_text(text, max_text_length)
            for text_portion in text_portions:
                translated_line_to_original_mapping[len(text_to_translate)] = 0
                text_to_translate.append(text_portion)
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
    while tries_translate_status < 3600:
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
                    f'text was too large to be translated, trying to split it up ...',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
                if max_text_length:
                    return translate_text(
                        text, source_language, target_language, graph_ai_server=graph_ai_server, sections=sections,
                        force=force, debug=debug, max_text_length=max_text_length-200,
                        max_text_list_length=max_text_list_length
                    )
                else:
                    return translate_text(
                        text, source_language, target_language, graph_ai_server=graph_ai_server,
                        sections=sections, force=force, debug=debug, max_text_length=2000,
                        max_text_list_length=max_text_list_length
                    )
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
                    if translated_text_full[original_line_idx] is None:
                        translated_text_full[original_line_idx] = translated_line
                    else:
                        translated_text_full[original_line_idx] += translated_line
                return translated_text_full
            elif max_text_length and isinstance(text, str) and isinstance(task_result['result'], list):
                return ''.join(task_result['result'])
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
