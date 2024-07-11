from json import loads, JSONDecodeError
from typing import Optional, List
from numpy import array
from graphai_client.utils import status_msg
from graphai_client.client_api.utils import call_async_endpoint, split_text, get_next_text_length_for_split

MIN_TEXT_LENGTH = 200
DEFAULT_MAX_TEXT_LENGTH_IF_TEXT_TOO_LONG = 400
STEP_AUTO_DECREASE_TEXT_LENGTH = 100


def embed_text(
        text: str, login_info: dict, model: str = None, force=False, sections=('GRAPHAI', 'EMBEDDING'),
        max_text_length=None, debug=False, quiet=False, max_tries=5, max_processing_time_s=600
) -> Optional[List[float]]:
    """
    Embed text using the specified model.

    :param text: text to embed.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param model: model to use for embedding ("all-MiniLM-L12-v2" by default).
    :param force: Should the cache be bypassed and the embedding forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param quiet: disable success status messages.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to perform the text extraction.
    :return: the embedding as a list of float.
    """
    text_length = len(text)
    if max_text_length and text_length > max_text_length:
        text_portions = split_text(text, max_text_length)
        if not quiet:
            status_msg(
                f'text length ({text_length}) was larger than max_text_length={max_text_length}, '
                f'split it up in {len(text_portions)} segments',
                color='yellow', sections=list(sections) + ['WARNING']
            )
        embedding_portions = []
        for text_portion in text_portions:
            embedding_portions.append(
                embed_text(
                    text_portion, login_info=login_info, model=model, force=force, sections=sections,
                    max_text_length=max_text_length, debug=debug, quiet=quiet, max_tries=max_tries,
                    max_processing_time_s=max_processing_time_s
                )
            )
        return array(embedding_portions).mean(axis=0)
    json_data = {"text": text, "force": force}
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
                f'text was too large to be embded, trying to split it up with max_text_length={max_text_length}...',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            return embed_text(
                text, login_info=login_info, model=model, force=force, sections=sections,
                max_text_length=max_text_length, debug=debug, quiet=quiet, max_tries=max_tries,
                max_processing_time_s=max_processing_time_s
            )
        return loads(task_result['result'])
    except JSONDecodeError as e:
        status_msg(
            f'Error while decoding the embedding: {str(e)}. '
            f'The result of the embedding task was: {task_result["result"]}',
            color='red', sections=sections + ('ERROR',)
        )


if __name__ == "__main__":
    from json import dump
    from graphai_client.client_api.utils import login
    from graphai_client.utils import get_piper_connection, execute_query

    limit = 100
    db = get_piper_connection('/home/yves/prog/graphai-client/graphai_client/config/graph_engine_db.json')
    login_info = login()
    embeddings = {}
    for object_type in ('Concept', 'Course', 'Lecture', 'MOOC', 'Person', 'Publication', 'Startup', 'Unit'):
        object_info = execute_query(
            db,
            f'''SELECT object_id, name_en_value, description_long_en_value 
                FROM graph_cache.Data_N_Object_T_PageProfileAll 
                WHERE object_type="{object_type}" AND description_long_en_value IS NOT NULL LIMIT {limit};'''
        )
        embeddings[object_type] = {
            object_id: {'name': name, 'description': description} for object_id, name, description in
            object_info
        }
        for model in ('all-MiniLM-L12-v2',):
            key_embed = 'embedding_' + model
            status_msg(
                f"Embed {len(embeddings[object_type])} {object_type}s...",
                sections=['EMBED', object_type.upper(), 'PROCESSING'], color='grey')
            for object_id, name, description in object_info:
                embeddings[object_type][object_id][key_embed] = embed_text(
                    name + '\n' + description, login_info, quiet=True, force=True, max_text_length=400
                )
            status_msg(
                f"Embedded {len(embeddings[object_type])} {object_type}s.",
                sections=['EMBED', object_type.upper(), 'DONE'], color='green'
            )
    with open('/home/yves/test_embeddings.json', 'wt') as fid:
        dump(embeddings, fid)