from json import loads, JSONDecodeError
from typing import Optional, List, Dict, Union
from numpy import array
from numpy.linalg import norm
from graphai_client.client_api.utils import (
    call_async_endpoint, split_text, get_next_text_length_for_split, limit_length_list_of_texts,
    limit_total_length_list_of_text, clean_list_of_texts, status_msg
)

MIN_TEXT_LENGTH = 128
DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG = 600
STEP_AUTO_DECREASE_TEXT_LENGTH = 100


def embed_text(
        text: Union[str, List[str]], login_info, model: str = None, sections=('GRAPHAI', 'EMBED'), **kwargs
) -> Optional[Union[Optional[List[float]], List[Optional[List[float]]]]]:
    """
    Embed the input text using the provided model.

    :param text: text to embed. Can be a string or a list of string.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param model: model to use for embedding ("all-MiniLM-L12-v2" by default).
    :param sections: sections to use in the status messages.
    :param kwargs: see embed_text_str() or embed_text_str() for the other parameters.
    :return: the embedding(s) of the input text if successful, None otherwise.
        If the input text was a string the output is a single embedding.
        If the input was a list, the output is a list of embeddings with the same number of elements than the input.
    """
    # handles str and list separately
    if isinstance(text, str):
        return embed_text_str(text, login_info, model, sections=sections, **kwargs)
    else:
        return embed_text_list(text, login_info, model, sections=sections, num_output=len(text), **kwargs)


def embed_text_str(
        text: str, login_info: dict, model: str = None, force=False, sections=('GRAPHAI', 'EMBEDDING'),
        max_text_length=None, debug=False, quiet=False, max_tries=5, max_processing_time_s=600,
        split_characters=('\n', '.', ';', ',', ' '), max_text_list_length=20000, no_cache=False
) -> Optional[List[float]]:
    """
    Embed text using the specified model.

    :param text: text to embed.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param model: model to use for embedding ("all-MiniLM-L12-v2" by default).
    :param force: Should the cache be bypassed and the embedding forced.
    :param sections: sections to use in the status messages.
    :param max_text_length: If the length of the text is greater than max_text_length, it is split
        according to the split_characters.
    :param debug: if True additional information about each connection to the API is displayed.
    :param quiet: disable success status messages.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to perform the text extraction.
    :param split_characters: list of characters ordered by priority (first=highest prio.) where the text can be split.
    :param max_text_list_length: the maximum cumulative length of the text list before the list is split.
    :param no_cache: if True, caching will be skipped. Set to True for sensitive data. False by default.
    :return: the embedding as a list of float.
    """
    text_length = len(text)
    if max_text_length and text_length > max_text_length:
        text_portions = split_text(text, max_text_length, split_characters=split_characters)
        if not quiet:
            status_msg(
                f'text length ({text_length}) was larger than max_text_length={max_text_length}, '
                f'split it up in {len(text_portions)} segments',
                color='yellow', sections=list(sections) + ['WARNING']
            )
        embedding_portions = embed_text_list(
            text_portions, login_info=login_info, model=model, force=force, sections=sections,
            max_text_length=max_text_length, debug=debug, max_tries=max_tries,
            max_processing_time_s=max_processing_time_s, split_characters=split_characters,
            mapping_from_input_to_original={i: 0 for i in range(len(text_portions))},
            num_output=1, max_text_list_length=max_text_list_length, no_cache=no_cache
        )
        return array(embedding_portions)[0]
    json_data = {"text": text, "force": force, "no_cache": no_cache}
    if model:
        json_data["model_type"] = model
    output_type = 'embedding'
    task_result = call_async_endpoint(
        endpoint='/embedding/embed',
        json=json_data,
        login_info=login_info,
        token=f"text ({len(text)} characters)",
        output_type=output_type,
        result_key='result',
        max_tries=max_tries,
        delay_retry=1,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        quiet=quiet,
        debug=debug
    )
    if task_result is None:
        return None
    try:
        if task_result.get('text_too_large', False):
            max_text_length = get_next_text_length_for_split(
                len(text), previous_text_length=max_text_length, text_length_min=MIN_TEXT_LENGTH,
                max_text_length_default=DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG,
                text_length_steps=STEP_AUTO_DECREASE_TEXT_LENGTH
            )
            status_msg(
                f'text was too large to be embed, trying to split it up with max_text_length={max_text_length}...',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            return embed_text_str(
                text, login_info=login_info, model=model, force=force, sections=sections,
                max_text_length=max_text_length, debug=debug, quiet=quiet, max_tries=max_tries,
                max_processing_time_s=max_processing_time_s, split_characters=split_characters,
                no_cache=no_cache
            )
        return loads(task_result['result'])
    except JSONDecodeError as e:
        status_msg(
            f'Error while decoding the embedding: {str(e)}. '
            f'The result of the embedding task was: {task_result["result"]}',
            color='red', sections=sections + ('ERROR',)
        )


def get_weights_embeddings(embeddings_to_original_mapping, length_of_texts_to_embed, num_output):
    length_original_texts = [0] * num_output
    for embed_index, orig_index in embeddings_to_original_mapping.items():
        length_original_texts[orig_index] += length_of_texts_to_embed[embed_index]
    return [
        length_of_texts_to_embed[embed_index] / length_original_texts[orig_index]
        for embed_index, orig_index in embeddings_to_original_mapping.items()
    ]


def embed_text_list(
        list_of_texts: List[Optional[str]], login_info: dict, model: str = None, force=False,
        max_text_length=None, debug=False, max_tries=5, max_processing_time_s=600, quiet=False,
        split_characters=('\n', '.', ';', ',', ' '), mapping_from_input_to_original: Optional[Dict[int, int]] = None,
        num_output: Optional[int] = None, max_text_list_length: Optional[int] = 20000, sections=('GRAPHAI', 'EMBEDDING'),
        no_cache: bool = False
) -> Optional[List[Optional[List[float]]]]:
    """
    Embed text using the specified model.

    :param list_of_texts: text to embed.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param model: model to use for embedding ("all-MiniLM-L12-v2" by default).
    :param force: Should the cache be bypassed and the embedding forced.
    :param sections: sections to use in the status messages.
    :param max_text_length: If the length of the text is greater than max_text_length, it is split
        according to the split_characters.
    :param debug: if True additional information about each connection to the API is displayed.
    :param quiet: disable success status messages.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to perform the text extraction.
    :param split_characters: list of characters ordered by priority (first=highest prio.) where the text can be split.
    :param mapping_from_input_to_original: mapping in case the input list of text has been previously split
    :param num_output: number of encoding to return. If None it will be determined by mapping_from_split_to_original
        or by the number of input.
    :param max_text_list_length: if not None, the list will be split in chunks where the total number of characters
    do not exceed that value. The list is then reformed after embedding.
    :param no_cache: if True, caching will be skipped. Set to True for sensitive data. False by default.
    :return: the embedding as a list of float.
    """
    if mapping_from_input_to_original is None and num_output is None:
        num_output = len(list_of_texts)
    # get rid of None in list input
    cleaned_text, mapping_from_cleaned_to_original = clean_list_of_texts(list_of_texts, mapping_from_input_to_original)
    length_of_input_texts = [len(text) for text in cleaned_text]
    embeddings_to_original_mapping = {}
    embeddings = []
    length_of_texts_to_embed = []
    if max_text_list_length is not None and sum(length_of_input_texts) > max_text_list_length:
        for text_list_split, mapping_after_list_split in limit_total_length_list_of_text(
                cleaned_text, max_text_list_length, mapping_from_cleaned_to_original
        ):
            length_of_texts_to_embed.extend([len(text) for text in text_list_split])
            if len(text_list_split) == 1:
                embedding_list_elem = embed_text_str(
                    text_list_split[0], login_info, model=model, sections=sections,
                    force=force, debug=debug, max_text_length=max_text_length,
                    max_tries=max_tries, max_processing_time_s=max_processing_time_s,
                    max_text_list_length=max_text_list_length, no_cache=no_cache
                )
                embeddings.append(embedding_list_elem)
            else:
                embedding_list_split = embed_text_list(
                    text_list_split, login_info, model=model, sections=sections,
                    force=force, debug=debug, max_text_length=max_text_length,
                    max_tries=max_tries, max_processing_time_s=max_processing_time_s,
                    mapping_from_input_to_original=None, num_output=len(text_list_split),
                    max_text_list_length=max_text_list_length, no_cache=no_cache
                )
                if embedding_list_split is None:
                    raise RuntimeError(f'failed to embed the following text list: {text_list_split}')
                embeddings.extend(embedding_list_split)
            embeddings_to_original_mapping.update(mapping_after_list_split)
        weights_embeddings = get_weights_embeddings(
            embeddings_to_original_mapping, length_of_texts_to_embed, num_output
        )
        return recombine_embeddings(
            embeddings, embeddings_to_original_mapping, output_length=num_output, weights=weights_embeddings
        )
    # split text too long
    texts_to_embed, embeddings_to_original_mapping = limit_length_list_of_texts(
        cleaned_text, max_text_length, mapping_from_cleaned_to_original, split_characters=split_characters
    )
    # compute embeddings weights
    if num_output is None:
        num_output = max(embeddings_to_original_mapping.values()) + 1
    length_of_texts_to_embed = [len(text) for text in texts_to_embed]
    weights_embeddings = get_weights_embeddings(embeddings_to_original_mapping, length_of_texts_to_embed, num_output)
    total_text_length = sum(length_of_input_texts)
    assert total_text_length == sum(length_of_texts_to_embed)
    task_result = call_async_endpoint(
        endpoint='/embedding/embed',
        json={"text": texts_to_embed, "force": force, "no_cache": no_cache},
        login_info=login_info,
        token=f'text ({total_text_length} characters in {len(texts_to_embed)} elements)',
        output_type='embeddings',
        result_key='result',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    # resubmit with a smaller value of max_text_length if we get a text_too_large error
    indices_text_too_long = []
    for index_item, task_result_item in enumerate(task_result):
        assert isinstance(task_result_item, dict)
        if task_result_item.get('text_too_large', False):
            indices_text_too_long.append(index_item)
    if indices_text_too_long:
        length_too_long = min([len(texts_to_embed[idx]) for idx in indices_text_too_long])
        max_text_length = get_next_text_length_for_split(
            length_too_long, previous_text_length=max_text_length, text_length_min=MIN_TEXT_LENGTH,
            max_text_length_default=DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG,
            text_length_steps=STEP_AUTO_DECREASE_TEXT_LENGTH
        )
        if not quiet:
            status_msg(
                f'{len(indices_text_too_long)} texts were too large to be embedded, '
                f'trying to split them up with max_text_length={max_text_length}...',
                color='yellow', sections=list(sections) + ['WARNING']
            )
        text_list_too_long = [
            cleaned_text[embeddings_to_original_mapping[too_long_embed_idx]]
            for too_long_embed_idx in indices_text_too_long
        ]
        embedding_texts_too_long = embed_text_list(
            text_list_too_long, login_info, model=model, sections=sections,
            force=force, debug=debug, max_text_length=max_text_length,
            max_tries=max_tries, max_processing_time_s=max_processing_time_s,
            mapping_from_input_to_original=None, num_output=len(text_list_too_long),
            max_text_list_length=max_text_list_length, no_cache=no_cache
        )
        result = [
            loads(task_result[ok_idx]['result']) if ok_idx not in indices_text_too_long
            else embedding_texts_too_long[indices_text_too_long.index(ok_idx)]
            for ok_idx in range(len(task_result))
        ]
        return recombine_embeddings(
            result, embeddings_to_original_mapping, output_length=num_output, weights=weights_embeddings
        )
    result = [loads(task_result_item['result']) for task_result_item in task_result]
    n_results = len(task_result)
    if n_results != len(result):
        if not force and not task_result.get('fresh', True):
            status_msg(
                f'invalid result for embeddings: the length of the embeddings {n_results} does not match '
                f'the length of the input {len(texts_to_embed)}, trying to force ...',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            return embed_text_list(
                cleaned_text, login_info, model=model, sections=sections,
                force=True, debug=debug, max_text_length=max_text_length,
                max_tries=max_tries, max_processing_time_s=max_processing_time_s,
                mapping_from_input_to_original=mapping_from_cleaned_to_original, num_output=num_output,
                max_text_list_length=max_text_list_length, no_cache=no_cache
            )
        else:
            raise RuntimeError(
                f'invalid result for embedding: the number of embeddings {n_results} does not match '
                f'the number of inputs {len(texts_to_embed)}'
            )
    # put back None in the output so the number of element is the same as in the input
    return recombine_embeddings(
        result, embeddings_to_original_mapping, output_length=num_output, weights=weights_embeddings
    )


def recombine_embeddings(
        list_of_embeddings: List[List[float]], mapping_from_split_to_original: Dict[int, int],
        output_length: Optional[int] = None, weights=None
) -> List[Optional[List[float]]]:
    list_of_embeddings = array(list_of_embeddings, dtype=float)
    if output_length is None:
        output_length = max(mapping_from_split_to_original.values()) + 1
    grouped_embeddings = [[] for _ in range(output_length)]
    for embedding_line, original_line in mapping_from_split_to_original.items():
        embedding = list_of_embeddings[embedding_line].copy()
        if weights is not None:
            embedding *= weights[embedding_line]
        grouped_embeddings[original_line].insert(embedding_line, embedding)
    recombined_embeddings = [
        array(group_of_embeddings_of_original_text).sum(axis=0) if group_of_embeddings_of_original_text else None
        for group_of_embeddings_of_original_text in grouped_embeddings
    ]
    recombined_embeddings_normalized = [
        (embedding / norm(embedding)).tolist() if embedding is not None else None for embedding in recombined_embeddings
    ]
    return recombined_embeddings_normalized
