from typing import Optional, Union
from graphai_client.utils import status_msg
from graphai_client.client_api.utils import call_async_endpoint

error_too_long = 'Unpunctuated text too long (over 512 tokens), ' \
                 'try adding punctuation or providing a smaller chunk of text.'


def translate_text(
        text: Union[str, list], source_language, target_language, login_info, sections=('GRAPHAI', 'TRANSLATE'),
        **kwargs
) -> Optional[Union[str, list]]:
    """
    Translate the input text from the source language to the target language.

    :param text: text to translate. Can be a string or a list of string.
    :param source_language: language of `text`.
    :param target_language: language of the output translated text.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param sections: sections to use in the status messages.
    :param force: Should the cache be bypassed and the translation forced.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_text_length: if not None, the text will be split in chunks smaller than max_text_length before being
        translated, it is glued together after translation. This happens automatically in case the API send a
        `text too long error`.
    :param max_text_list_length: if not None and the input is a list, the list will be translated in chunks where the
        total number of characters do not exceed that value. The list is then reformed after translation.
    :param n_try: the number of tries before giving up.
    :param delay_retry: the time to wait between tries.
    :return: the translated text if successful, None otherwise.
        If the input text was a string the output is a string too.
        If the input was a list, the output is a list with the same number of elements than the input.
    """
    if source_language == target_language:
        return text
    # use english as an intermediary if needed
    if source_language not in ('en', 'fr') and target_language != 'en':
        translated_text_en = translate_text(
            text, source_language, 'en', login_info, sections=sections, **kwargs
        )
        if translated_text_en is None:
            status_msg(
                f'failed to translate "{text}" from {source_language} into en',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            return None
        return translate_text(
            translated_text_en, 'en', target_language, login_info, sections=sections, **kwargs
        )
    # handles str and list separately
    if isinstance(text, str):
        return translate_text_str(
            text, source_language, target_language, login_info, sections=sections, **kwargs
        )
    else:
        return translate_text_list(
            text, source_language, target_language, login_info, sections=sections, **kwargs
        )


def translate_text_str(
        text: str, source_language, target_language, login_info,
        sections=('GRAPHAI', 'TRANSLATE'), force=False, debug=False, max_text_length=None, max_text_list_length=20000,
        n_try=600, delay_retry=1
) -> Optional[str]:
    """
    Translate the input text from the source language to the target language.

    :param text: text to translate.
    :param source_language: language of `text`.
    :param target_language: language of the output translated text.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param sections: sections to use in the status messages.
    :param force: Should the cache be bypassed and the translation forced.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_text_length: if not None, the text will be split in chunks smaller than max_text_length before being
        translated, it is glued together after translation. This happens automatically in case the API send a
        `text too long error`.
    :param max_text_list_length: if not None and the input is a list, the list will be translated in chunks where the
        total number of characters do not exceed that value. The list is then reformed after translation.
    :param n_try: the number of tries before giving up.
    :param delay_retry: the time to wait between tries.
    :return: the translated text if successful, None otherwise.
    """
    # check for empty text
    if not text:
        return text
    # split text into a list of string if the input is a too long string
    if max_text_length and len(text) > max_text_length:
        translated_line_to_original_mapping = {}
        text_to_translate = []
        text_portions = split_text(text, max_text_length)
        for text_portion in text_portions:
            translated_line_to_original_mapping[len(text_to_translate)] = 0
            text_to_translate.append(text_portion)
        translated_list = translate_text_list(
            text_to_translate, source_language, target_language, login_info,
            sections=sections, force=force, debug=debug, max_text_length=max_text_length,
            max_text_list_length=max_text_list_length, n_try=n_try, delay_retry=delay_retry
        )
        return ''.join(translated_list)
    task_result = call_async_endpoint(
        endpoint='/translation/translate',
        json={"text": text, "source": source_language, "target": target_language, "force": force},
        login_info=login_info,
        token=source_language + ' text',
        output_type='translation',
        n_try=n_try,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    # resubmit with a smaller value of max_text_length if we get a text_too_large error
    if task_result['text_too_large'] or task_result['result'] == error_too_long:
        if task_result['result'] == error_too_long:
            force = True
        if max_text_length is None:
            max_text_length = 2000
        else:
            max_text_length = max_text_length - 200
        status_msg(
            f'text was too large to be translated, trying to split it up with max_text_length={max_text_length}...',
            color='yellow', sections=list(sections) + ['WARNING']
        )
        return translate_text_str(
            text, source_language, target_language, login_info, sections=sections,
            force=force, debug=debug, max_text_length=max_text_length,
            max_text_list_length=max_text_list_length
        )
    return task_result['result']


def translate_text_list(
        text: list, source_language, target_language, login_info,
        sections=('GRAPHAI', 'TRANSLATE'), force=False, debug=False, max_text_length=None, max_text_list_length=20000,
        n_try=600, delay_retry=1
) -> Optional[list]:
    """
    Translate the input text (as a list) from the source language to the target language.

    :param text: list of text to translate.
    :param source_language: language of `text`.
    :param target_language: language of the output translated text.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param sections: sections to use in the status messages.
    :param force: Should the cache be bypassed and the translation forced.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_text_length: if not None, the text will be split in chunks smaller than max_text_length before being
        translated, it is glued together after translation. This happens automatically in case the API send a
        `text too long error`.
    :param max_text_list_length: if not None and the input is a list, the list will be translated in chunks where the
        total number of characters do not exceed that value. The list is then reformed after translation.
    :param n_try: the number of tries before giving up.
    :param delay_retry: the time to wait between tries.
    :return: the translated text if successful, None otherwise. The length of the returned list is the same as for text
    """
    # check for list of empty text
    is_empty = True
    for line in text:
        if line and line.strip():
            is_empty = False
            break
    if is_empty:
        return text
    # split in smaller lists if the list is too large
    lengths_text = [len(t) if t is not None else 0 for t in text]
    if sum(lengths_text) > max_text_list_length:
        idx_start = 0
        sum_length = 0
        n_text_elems = len(text)
        translated_text_full = [None] * n_text_elems
        for idx_end in range(n_text_elems):
            sum_length += lengths_text[idx_end]
            # we reached the end
            if idx_end + 1 == n_text_elems:
                status_msg(
                    f'get part of the text (from {idx_start} to the {n_text_elems}) as the full list is too long',
                    color='grey', sections=list(sections) + ['PROCESSING']
                )
                translated_text_full[idx_start:] = translate_text_list(
                    text[idx_start:], source_language, target_language, login_info,
                    sections=sections, force=force, debug=debug, max_text_length=max_text_length,
                    max_text_list_length=max_text_list_length
                )
            # one element is already too large
            elif sum_length > max_text_list_length:
                status_msg(
                    f'get part of the text (at {idx_start}) as the full list is too long',
                    color='grey', sections=list(sections) + ['PROCESSING']
                )
                translated_text_full[idx_start] = translate_text_str(
                    text[idx_start], source_language, target_language, login_info,
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
                translated_text_full[idx_start:idx_end + 1] = translate_text_list(
                    text[idx_start:idx_end + 1], source_language, target_language, login_info,
                    sections=sections, force=force, debug=debug, max_text_length=max_text_length,
                    max_text_list_length=max_text_list_length
                )
                idx_start = idx_end + 1
                sum_length = 0
        return translated_text_full
    # get rid of None in list input and split text too long
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
    task_result = call_async_endpoint(
        endpoint='/translation/translate',
        json={"text": text_to_translate, "source": source_language, "target": target_language, "force": force},
        login_info=login_info,
        token=source_language + ' text',
        output_type='translation',
        n_try=n_try,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    # resubmit with a smaller value of max_text_length if we get a text_too_large error
    any_text_too_large = False
    for result_str in task_result['result']:
        if result_str == error_too_long:
            any_text_too_large = True
    if any_text_too_large:
        if max_text_length is None:
            max_text_length = 2000
        else:
            max_text_length = max_text_length - 200
        status_msg(
            f'text was too large to be translated, trying to split it up with max_text_length={max_text_length}...',
            color='yellow', sections=list(sections) + ['WARNING']
        )
        return translate_text_list(
            text, source_language, target_language, login_info, sections=sections,
            force=force, debug=debug, max_text_length=max_text_length,
            max_text_list_length=max_text_list_length
        )
    # put back None in the output so the number of element is the same as in the input
    translated_text_full = [None] * len(text)
    for tr_line_idx, translated_line in enumerate(task_result['result']):
        original_line_idx = translated_line_to_original_mapping[tr_line_idx]
        if translated_text_full[original_line_idx] is None:
            translated_text_full[original_line_idx] = translated_line
        else:
            translated_text_full[original_line_idx] += translated_line
    return translated_text_full


def detect_language(
        text: str, login_info, sections=('GRAPHAI', 'TRANSLATE'), debug=False, n_try=120, delay_retry=1, quiet=True
) -> Optional[str]:
    """
    Detect the language of the given text.

    :param text: text for which the language has to be detected.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param n_try: the number of tries before giving up.
    :param delay_retry: the time to wait between tries.
    :param quiet: disable success status messages.
    :return: the detected language if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/translation/detect_language',
        json={"text": text},
        login_info=login_info,
        token=f'"{text}"',
        output_type='language',
        n_try=n_try,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug,
        quiet=quiet
    )
    if task_result is None:
        return None
    return task_result['language']


def split_text(text: str, max_length: int, split_characters=('\n', '.', ';', ',', ' ')):
    result = []
    assert max_length > 0
    while len(text) > max_length:
        for split_char in split_characters:
            pos = text[:max_length].rfind(split_char)
            if pos > 0:
                result.append(text[:pos+1])
                text = text[pos+1:]
                break
        if len(text) > max_length:
            result.append(text[:max_length])
            text = text[max_length:]
    if len(text) > 0:
        result.append(text)
    return result
