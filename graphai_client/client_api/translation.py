from re import match
from typing import Optional, Union, Dict, List
from graphai_client.client_api.utils import (
    call_async_endpoint, get_next_text_length_for_split, split_text, limit_length_list_of_texts, clean_list_of_texts,
    recombine_split_list_of_texts, status_msg
)

MIN_TEXT_LENGTH = 500
DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG = 4000
STEP_AUTO_DECREASE_TEXT_LENGTH = 200


def translate_text(
        text: Union[str, List[Optional[str]]], source_language, target_language, login_info,
        sections=('GRAPHAI', 'TRANSLATE'), **kwargs
) -> Optional[Union[str, List[Optional[str]]]]:
    """
    Translate the input text from the source language to the target language.

    :param text: text to translate. Can be a string or a list of string.
    :param source_language: language of `text`.
    :param target_language: language of the output translated text.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param sections: sections to use in the status messages.
    :param kwargs: see translate_text_str() or translate_text_list() for the other parameters.
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
        max_tries=5, max_processing_time_s=3600, delay_retry=1, no_cache=False
) -> Optional[str]:
    """
    Translate the input text from the source language to the target language.
    Split the text into a list of string if the input is too long

    :param text: text to translate.
    :param source_language: language of `text`.
    :param target_language: language of the output translated text.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param sections: sections to use in the status messages.
    :param force: Should the cache be bypassed and the translation forced.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_text_length: if not None, the text will be split in chunks smaller than max_text_length before being
        translated, it is glued together after translation. This happens automatically in case the API send a
        `text too large error`.
    :param max_text_list_length: if not None and the input is a list, the list will be translated in chunks where the
        total number of characters do not exceed that value. The list is then reformed after translation.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to perform the translation.
    :param delay_retry: time to wait before retrying after an error
    :param no_cache: if True, caching will be skipped. Set to True for sensitive data. False by default.
    :return: the translated text if successful, None otherwise.
    """
    # check for empty text
    if not text:
        return text
    # split text into a list of string if the input is a too long string
    if max_text_length and len(text) > max_text_length:
        text_to_translate = split_text(text, max_text_length)
        translated_list = translate_text_list(
            text_to_translate, source_language, target_language, login_info, sections=sections, force=force,
            debug=debug, max_text_length=max_text_length,  max_text_list_length=max_text_list_length,
            max_tries=max_tries, max_processing_time_s=max_processing_time_s, delay_retry=delay_retry,
            mapping_from_input_to_original={i: 0 for i in range(len(text_to_translate))}, no_cache=no_cache
        )
        return ''.join(translated_list)
    task_result = call_async_endpoint(
        endpoint='/translation/translate',
        json={
            "text": text,
            "source": source_language,
            "target": target_language,
            "force": force,
            "no_cache": no_cache
        },
        login_info=login_info,
        token=f'{source_language} text ({len(text)} characters)',
        output_type=target_language + ' translation',
        result_key='result',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    # resubmit with a smaller value of max_text_length if we get a text_too_large error
    if task_result['text_too_large']:
        max_text_length = get_next_text_length_for_split(
            len(text), previous_text_length=max_text_length, text_length_min=MIN_TEXT_LENGTH,
            max_text_length_default=DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG,
            text_length_steps=STEP_AUTO_DECREASE_TEXT_LENGTH
        )
        status_msg(
            f'text was too large to be translated, trying to split it up with max_text_length={max_text_length}...',
            color='yellow', sections=list(sections) + ['WARNING']
        )
        return translate_text_str(
            text, source_language, target_language, login_info, sections=sections,
            force=force, debug=debug, max_text_length=max_text_length, max_text_list_length=max_text_list_length,
            max_tries=max_tries, max_processing_time_s=max_processing_time_s, delay_retry=delay_retry, no_cache=no_cache
        )
    return task_result['result']


def translate_text_list(
        list_of_texts: List[Optional[str]], source_language: str, target_language: str, login_info: dict,
        sections=('GRAPHAI', 'TRANSLATE'), force=False, debug=False, max_text_length=None, max_text_list_length=20000,
        max_tries=5, max_processing_time_s=3600, delay_retry=1,
        mapping_from_input_to_original: Optional[Dict[int, int]] = None,
        num_output: Optional[int] = None,
        no_cache: bool = False
) -> Optional[list]:
    """
    Translate the list of text from the sourc
    e language to the target language.

    :param list_of_texts: list of text to translate.
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
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to perform the translation.
    :param delay_retry: time to wait before retrying after an error
    :param mapping_from_input_to_original: mapping in case the input list of text has been previously split
    :param no_cache: if True, caching will be skipped. Set to True for sensitive data. False by default.
    :return: the translated text if successful, None otherwise. The length of the returned list is the same as for text
    """
    if mapping_from_input_to_original is None and num_output is None:
        num_output = len(list_of_texts)
    # get rid of None in list input
    cleaned_texts, mapping_from_cleaned_to_original = clean_list_of_texts(list_of_texts, mapping_from_input_to_original)
    if num_output is None:
        max(mapping_from_cleaned_to_original.values()) + 1
    # check for list of empty text
    is_empty = True
    for line in cleaned_texts:
        if line and line.strip():
            is_empty = False
            break
    if is_empty:
        return cleaned_texts
    # split in smaller lists if the list is too large
    lengths_text = [len(t) if t is not None else 0 for t in cleaned_texts]
    total_text_length = sum(lengths_text)
    if total_text_length > max_text_list_length:
        idx_start = 0
        sum_length = 0
        n_text_elems = len(cleaned_texts)
        translated_text_full = [None] * n_text_elems
        for idx_end in range(n_text_elems):
            sum_length += lengths_text[idx_end]
            # we reached the end
            if idx_end + 1 == n_text_elems:
                status_msg(
                    f'get part of the text (from {idx_start} to the {n_text_elems}/{n_text_elems}) '
                    f'as the full list is too long',
                    color='grey', sections=list(sections) + ['PROCESSING']
                )
                translated_text_part = translate_text_list(
                    cleaned_texts[idx_start:], source_language, target_language, login_info,
                    sections=sections, force=force, debug=debug, max_text_length=max_text_length,
                    max_text_list_length=max_text_list_length, max_tries=max_tries,
                    max_processing_time_s=max_processing_time_s, delay_retry=delay_retry,
                    mapping_from_input_to_original=None, no_cache=no_cache
                )
                if translated_text_part is None:
                    return None
                translated_text_full[idx_start:] = translated_text_part
            # one element is already too large
            elif sum_length > max_text_list_length:
                status_msg(
                    f'get part of the text (at {idx_start}/{n_text_elems}) as the full list is too long',
                    color='grey', sections=list(sections) + ['PROCESSING']
                )
                translated_text_full[idx_start] = translate_text_str(
                    cleaned_texts[idx_start], source_language, target_language, login_info,
                    sections=sections, force=force, debug=debug, max_text_length=max_text_length,
                    max_text_list_length=max_text_list_length, max_tries=max_tries,
                    max_processing_time_s=max_processing_time_s, delay_retry=delay_retry,
                    no_cache=no_cache
                )
                idx_start += 1
                sum_length = 0
            # with the next element it is too large, or we reached the end
            elif sum_length + lengths_text[idx_end + 1] > max_text_list_length:
                status_msg(
                    f'get part of the text (from {idx_start} to {idx_end}/{n_text_elems}) as the full list is too long',
                    color='grey', sections=list(sections) + ['PROCESSING']
                )
                translated_text_part = translate_text_list(
                    cleaned_texts[idx_start:idx_end + 1], source_language, target_language, login_info,
                    sections=sections, force=force, debug=debug, max_text_length=max_text_length,
                    max_text_list_length=max_text_list_length, max_tries=max_tries,
                    max_processing_time_s=max_processing_time_s, delay_retry=delay_retry,
                    mapping_from_input_to_original=None, no_cache=no_cache
                )
                if translated_text_part is None:
                    return None
                translated_text_full[idx_start:idx_end + 1] = translated_text_part
                idx_start = idx_end + 1
                sum_length = 0
        return recombine_split_list_of_texts(
            translated_text_full, mapping_from_cleaned_to_original, output_length=num_output
        )
    # split text too long
    text_to_translate, translated_line_to_original_mapping = limit_length_list_of_texts(
        cleaned_texts, max_text_length, mapping_from_cleaned_to_original
    )
    task_result = call_async_endpoint(
        endpoint='/translation/translate',
        json={
            "text": text_to_translate,
            "source": source_language,
            "target": target_language,
            "force": force,
            "no_cache": no_cache
        },
        login_info=login_info,
        token=f'{source_language} text ({total_text_length} characters in {len(text_to_translate)} elements)',
        output_type=target_language + ' translation',
        result_key='result',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    # resubmit with a smaller value of max_text_length if we get a text_too_large error
    if task_result.get('text_too_large', False):
        match_indices = match(r'.*This happened for inputs at indices ((?:\d+, )*\d+)\.', task_result['result'])
        if match_indices:
            indices_text_too_long = [int(idx) for idx in match_indices.group(1).split(', ') if idx is not None]
            length_too_long = min([len(text_to_translate[idx]) for idx in indices_text_too_long])
        else:
            length_too_long = DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG
        max_text_length = get_next_text_length_for_split(
            length_too_long, previous_text_length=max_text_length, text_length_min=MIN_TEXT_LENGTH,
            max_text_length_default=DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG,
            text_length_steps=STEP_AUTO_DECREASE_TEXT_LENGTH
        )
        status_msg(
            f'text was too large to be translated, trying to split it up with max_text_length={max_text_length}...',
            color='yellow', sections=list(sections) + ['WARNING']
        )
        return translate_text_list(
            cleaned_texts, source_language, target_language, login_info, sections=sections,
            force=True, debug=debug, max_text_length=max_text_length, max_text_list_length=max_text_list_length,
            max_tries=max_tries, max_processing_time_s=max_processing_time_s, delay_retry=delay_retry,
            mapping_from_input_to_original=mapping_from_cleaned_to_original, no_cache=no_cache
        )
    n_results = len(task_result['result'])
    if n_results != len(text_to_translate):
        if not force and not task_result.get('fresh', True):
            status_msg(
                f'invalid result for translation: the length of the translation {n_results} does not match '
                f'the length of the input {len(text_to_translate)}, trying to force ...',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            return translate_text_list(
                cleaned_texts, source_language, target_language, login_info, sections=sections,
                force=True, debug=debug, max_text_length=max_text_length, max_text_list_length=max_text_list_length,
                max_tries=max_tries, max_processing_time_s=max_processing_time_s, delay_retry=delay_retry,
                mapping_from_input_to_original=mapping_from_cleaned_to_original, no_cache=no_cache
            )
        else:
            raise RuntimeError(
                f'invalid result for translation: the length of the translation {n_results} does not match '
                f'the length of the input {len(text_to_translate)}'
            )
    # put back None in the output so the number of element is the same as in the input
    return recombine_split_list_of_texts(
        task_result['result'], translated_line_to_original_mapping, output_length=num_output
    )


def detect_language(
        text: str, login_info, sections=('GRAPHAI', 'TRANSLATE'), debug=False, max_tries=5, max_processing_time_s=120,
        delay_retry=1, quiet=True
) -> Optional[str]:
    """
    Detect the language of the given text.

    :param text: text for which the language has to be detected.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to perform the language detection.
    :param delay_retry: time to wait before retrying after an error
    :param quiet: disable success status messages.
    :return: the detected language if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/translation/detect_language',
        json={"text": text},
        login_info=login_info,
        token=f'"{text}"',
        output_type='language',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug,
        quiet=quiet
    )
    if task_result is None:
        return None
    return task_result['language']
