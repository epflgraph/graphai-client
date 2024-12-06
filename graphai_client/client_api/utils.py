from json import load as load_json
from os.path import normpath, join, dirname, exists
from re import fullmatch
from time import sleep
from datetime import datetime, timedelta
from requests import get, post, Response
from termcolor import cprint
from typing import Callable, Dict, Optional, Tuple, List


def call_async_endpoint(
        endpoint, json, login_info, token, output_type, result_key=None, max_processing_time_s=6000, max_tries=5,
        delay_retry=1, sections=(), debug=False, quiet=False, _tries=1
):
    """
    Helper for asynchronous API endpoints. It first sends json data to the endpoint, get the task_id from the response
    then query the status endpoint until it gets a 'SUCCESS' in 'task_status' and then return the content of
    'task_status'. In case of error, the full process is tried up to max_tries times.

    :param endpoint: API endpoint (f.e. '/image/extract_text')
    :param json: json data to send to the endpoint (f.e. '{"token": "ex_token", "method": "google", "force": True}')
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param token: label of the input, this parameter is only used for logs.
    :param output_type: type of output, this parameter is only used for logs.
    :param result_key: key of the main result, this parameter is only used for logs or if the input text is too long.
    :param max_processing_time_s: maximum number of seconds to wait for a successful task status after starting a task.
    :param max_tries: maximum number of time the task should be tried.
    :param delay_retry: time waited between status checks and before a new trial after an error.
    :param sections: sections to use in the status messages, this parameter is only used for logs.
    :param debug: if True additional information about each connection to the API is displayed.
    :param quiet: if True no logs are displayed.
    :param _tries: current try number (internal usage).
    :return: the content of the 'task_result' key from the response of the status endpoint.
    """
    if not quiet:
        status_msg(
            f'extracting {output_type} from {token}...',
            color='grey', sections=list(sections) + ['PROCESSING']
        )
    response_endpoint = _get_response(
        url=endpoint,
        login_info=login_info,
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json=json,
        max_tries=max_tries,
        timeout=60,
        sections=sections,
        debug=debug,
    )
    if response_endpoint is None:
        status_msg(
            f'Got unexpected None response calling {endpoint} with the following data: {json}',
            color='yellow', sections=list(sections) + ['WARNING']
        )
        return None
    # get task_id to poll for result
    task_id = response_endpoint.json()['task_id']
    # wait for the task to be completed
    max_processing_time = timedelta(seconds=max_processing_time_s)
    start = datetime.now()
    while datetime.now() - start < max_processing_time and _tries <= max_tries:
        response_status = _get_response(
            url=f'{endpoint}/status/{task_id}',
            login_info=login_info,
            request_func=get,
            headers={'Content-Type': 'application/json'},
            max_tries=max_tries,
            timeout=60,
            sections=sections,
            debug=debug,
        )
        if response_status is None:
            if not quiet or _tries == max_tries:
                status_msg(
                    f'Got unexpected None response calling {endpoint}/status/{task_id} at try {_tries}/{max_tries}. '
                    f'The data sent to {endpoint} was: {json}',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
            _tries += 1
            sleep(delay_retry)
            continue
        response_status_json = response_status.json()
        if not isinstance(response_status_json, dict):
            if not quiet or _tries == max_tries:
                status_msg(
                    f'Got unexpected response_status: {response_status_json} while extracting {output_type} '
                    f'from {token} at try {_tries}/{max_tries}', color='yellow', sections=list(sections) + ['WARNING']
                )
            _tries += 1
            sleep(delay_retry)
            continue
        task_status = response_status_json['task_status']
        if task_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif task_status == 'SUCCESS':
            task_result = response_status_json.get('task_result', None)
            if task_result is None or (not isinstance(task_result, dict) and not isinstance(task_result, list)):
                if not quiet or _tries == max_tries:
                    status_msg(
                        f'Bad task result "{task_result}" while extracting {output_type} from {token} '
                        f'at try {_tries}/{max_tries}',
                        color='yellow', sections=list(sections) + ['WARNING']
                    )
                _tries += 1
                sleep(delay_retry)
                continue
            if isinstance(task_result, list):
                indices_not_successful = []
                indices_text_too_large = []
                for index_result, task_result_item in enumerate(task_result):
                    assert isinstance(task_result_item, dict)
                    if not task_result_item.get('successful', True):
                        if task_result_item.get('text_too_large', False):
                            indices_text_too_large.append(index_result)
                            continue
                        indices_not_successful.append(index_result)
                if indices_text_too_large:
                    return task_result  # text too large to be handled by calling function
                if indices_not_successful:
                    if not quiet or _tries == max_tries:
                        status_msg(
                            f'extraction of the {output_type} from {token} failed at try {_tries}/{max_tries}',
                            color='yellow', sections=list(sections) + ['WARNING']
                        )
                    sleep(delay_retry)
                    return call_async_endpoint(
                        endpoint, json, login_info, token, output_type, max_processing_time_s=max_processing_time_s,
                        _tries=_tries + 1, max_tries=max_tries, delay_retry=delay_retry,
                        sections=sections, debug=debug, quiet=quiet
                    )
                return task_result
            assert isinstance(task_result, dict)
            if not task_result.get('successful', True):
                if task_result.get('text_too_large', False):
                    return task_result  # text too large to be handled by calling function
                if not quiet or _tries == max_tries:
                    status_msg(
                        f'extraction of the {output_type} from {token} failed at try {_tries}/{max_tries}',
                        color='yellow', sections=list(sections) + ['WARNING']
                    )
                sleep(delay_retry)
                return call_async_endpoint(
                    endpoint, json, login_info, token, output_type, max_processing_time_s=max_processing_time_s,
                    _tries=_tries + 1, max_tries=max_tries, delay_retry=delay_retry,
                    sections=sections, debug=debug, quiet=quiet
                )
            display_status_for_task_result(task_result, result_key, token, output_type, list(sections), quiet)
            return task_result
        elif task_status == 'FAILURE':
            if not quiet or _tries == max_tries:
                status_msg(
                    f'Calling {endpoint} caused a failure at try {_tries}/{max_tries}. '
                    f'The response was:\n{response_status_json}\nThe data was:\n{json}',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
            sleep(delay_retry)
            return call_async_endpoint(
                endpoint, json, login_info, token, output_type, max_processing_time_s=max_processing_time_s,
                _tries=_tries + 1, max_tries=max_tries, delay_retry=delay_retry,
                sections=sections, debug=debug, quiet=quiet
            )
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of {endpoint} for {token} at try {_tries}/{max_tries}: '
                + task_status
            )
    if _tries > max_tries:
        msg = f'Maximum try {max_tries}/{max_tries} reached for {endpoint} with the following json data: \n{json}'
    elif datetime.now() - start > max_processing_time:
        msg = f'Timeout of {max_processing_time_s}s reached for {endpoint} with the following json data: \n{json}'
    else:
        msg = f'Unknown failure for {endpoint} with the following json data: \n{json}'
    status_msg(msg, color='yellow', sections=list(sections) + ['WARNING'])
    return None


def limit_total_length_list_of_text(
        list_of_texts: List[str], max_total_text_length: Optional[int] = None,
        mapping_from_split_to_original: Optional[Dict[int, int]] = None
):
    if mapping_from_split_to_original is None:
        mapping_from_split_to_original = {i: i for i in range(len(list_of_texts))}
    lengths_text = [len(t) if t is not None else 0 for t in list_of_texts]
    total_text_length = sum(lengths_text)
    if total_text_length > max_total_text_length:
        idx_start = 0
        sum_length = 0
        n_text_elems = len(list_of_texts)
        for idx_end in range(n_text_elems):
            sum_length += lengths_text[idx_end]
            # we reached the end
            if idx_end + 1 == n_text_elems:
                yield (
                    list_of_texts[idx_start:],
                    {
                        split_idx: mapping_from_split_to_original[split_idx]
                        for split_idx in range(idx_start, n_text_elems)
                    }
                )
                return
            # one element is already too large
            elif lengths_text[idx_start] > max_total_text_length:
                yield [list_of_texts[idx_start]], {idx_start: mapping_from_split_to_original[idx_start]}
                idx_start += 1
                sum_length = 0
            # with the next element the list becomes too large
            elif sum_length + lengths_text[idx_end + 1] > max_total_text_length:
                yield (
                    list_of_texts[idx_start:idx_end + 1],
                    {
                        split_idx: mapping_from_split_to_original[split_idx]
                        for split_idx in range(idx_start, idx_end + 1)
                    }
                )
                idx_start = idx_end + 1
                sum_length = 0
    else:
        yield list_of_texts, mapping_from_split_to_original


def limit_length_list_of_texts(
        list_of_texts: List[Optional[str]], max_text_length: Optional[int] = None,
        mapping_from_split_to_original: Optional[Dict[int, int]] = None,
        split_characters=('\n', '.', ';', ',', ' ')
) -> Tuple[List[str], Dict[int, int]]:
    if mapping_from_split_to_original is None:
        mapping_from_split_to_original = {i: i for i in range(len(list_of_texts))}
    if max_text_length is None:
        return list_of_texts, mapping_from_split_to_original
    new_mapping_from_split_to_original = {}
    list_of_texts_split = []
    # for original_line_idx, text in enumerate(list_of_texts):
    for original_line_idx in sorted(set(mapping_from_split_to_original.values())):
        # get the split lines corresponding to the original line
        split_line_indices = sorted([
            split_idx for split_idx, orig_idx in mapping_from_split_to_original.items() if orig_idx == original_line_idx
        ])
        # check if any is too long
        is_too_long = False
        for split_idx in split_line_indices:
            if len(list_of_texts[split_idx]) > max_text_length:
                is_too_long = True
                break
        # if any is too long, we recombine the original text and split it with the given max_text_length
        if is_too_long:
            text = ''.join([list_of_texts[idx] for idx in split_line_indices])
            split_line = split_text(text, max_text_length, split_characters=split_characters)
            for line_portion in split_line:
                new_mapping_from_split_to_original[len(list_of_texts_split)] = original_line_idx
                list_of_texts_split.append(line_portion)
        # otherwise we just append those lines to the result
        else:
            for split_idx in split_line_indices:
                new_mapping_from_split_to_original[len(list_of_texts_split)] = original_line_idx
                list_of_texts_split.append(list_of_texts[split_idx])
    return list_of_texts_split, new_mapping_from_split_to_original


def clean_list_of_texts(
        list_of_texts: List[Optional[str]], mapping_from_input_to_original: Optional[Dict[int, int]] = None
) -> Tuple[List[str], Dict[int, int]]:
    if mapping_from_input_to_original is None:
        mapping_from_input_to_original = {i: i for i in range(len(list_of_texts))}
    indices_to_keep = [
        idx for idx in range(len(list_of_texts))
        if (list_of_texts[idx] is not None and isinstance(list_of_texts[idx], str) and len(list_of_texts[idx]) > 0)
    ]
    cleaned_list_of_texts = [list_of_texts[idx] for idx in indices_to_keep]
    mapping_from_clean_to_original = {
        pos: mapping_from_input_to_original[idx_to_keep] for pos, idx_to_keep in enumerate(indices_to_keep)
    }
    return cleaned_list_of_texts, mapping_from_clean_to_original


def recombine_split_list_of_texts(
        list_of_texts_split: List[Optional[str]], mapping_from_split_to_original: Optional[Dict[int, int]],
        output_length: Optional[int] = None
) -> List[Optional[str]]:
    if mapping_from_split_to_original is None:
        mapping_from_split_to_original = {i: i for i in range(len(list_of_texts_split))}
    if output_length is None:
        output_length = max(mapping_from_split_to_original.values()) + 1
    recombined_list_of_texts: List[Optional[str]] = [None] * output_length
    for tr_line_idx, translated_line in enumerate(list_of_texts_split):
        original_line_idx = mapping_from_split_to_original[tr_line_idx]
        if recombined_list_of_texts[original_line_idx] is None:
            recombined_list_of_texts[original_line_idx] = translated_line
        else:
            recombined_list_of_texts[original_line_idx] += translated_line
    return recombined_list_of_texts


def get_next_text_length_for_split(
        text_length: int, previous_text_length=None, text_length_min=400, max_text_length_default=4000,
        text_length_steps=200
):
    if previous_text_length == text_length_min:
        raise ValueError(
            f'Got a text_too_long error while max_text_length is at the minimum ({text_length_min} characters).'
        )
    if previous_text_length is None:
        text_length = min(
            max_text_length_default,
            max(text_length_min, text_length - text_length_steps)
        )
    else:
        text_length = max(previous_text_length - text_length_steps, text_length_min)
    return text_length


def split_text(text: str, max_length: int, split_characters=('\n', '.', ';', ',', ' ')) -> List[str]:
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


def display_status_for_task_result(
        task_result: dict, result_key: str, token: str, output_type: str, sections: list, quiet: bool
) -> None:
    def _incr_active_and_fingerprinted(
            task_res: dict, output_id: str, n_missing: int, n_active: int, n_fingerprinted: int
    ) -> Tuple[int, int, int]:
        if not isinstance(task_res, dict):
            return n_missing + 1, n_active, n_fingerprinted
        token_status = task_res.get('token_status', None)
        if token_status is None:
            return n_missing + 1, n_active, n_fingerprinted
        if isinstance(token_status, dict):
            active = token_status.get('active', False)
            fingerprinted = token_status.get('fingerprinted', False)
            return n_missing, n_active + (1 if active else 0), n_fingerprinted + (1 if fingerprinted else 0)
        else:
            raise RuntimeError(f'invalid type of token_status: {type(token_status)} for {output_id}')

    def _count_active_and_fingerprinted() -> Tuple[int, int, int, int]:
        n_result = 1
        result = task_result.get(result_key, None)
        if isinstance(result, dict):
            n_result = len(result)
        elif isinstance(result, list) or isinstance(result, tuple):
            n_result = len(result)
        n_missing_token_status = 0
        n_active = 0
        n_fingerprinted = 0
        if isinstance(result, dict):
            for key, res in result.items():
                n_missing_token_status, n_active, n_fingerprinted = _incr_active_and_fingerprinted(
                    res, f'{output_type} {key} from {token}', n_missing_token_status, n_active, n_fingerprinted
                )
        elif isinstance(result, list) or isinstance(result, tuple):
            for idx, res in enumerate(result):
                n_missing_token_status, n_active, n_fingerprinted = _incr_active_and_fingerprinted(
                    res, f'{output_type} {idx} from {token}', n_missing_token_status, n_active, n_fingerprinted
                )
        else:
            n_missing_token_status, n_active, n_fingerprinted = _incr_active_and_fingerprinted(
                task_result, f'{output_type} from {token}', n_missing_token_status, n_active, n_fingerprinted
            )
        return n_result, n_missing_token_status, n_active, n_fingerprinted

    device = task_result.get('device', None)
    token_size: int | None = task_result.get('token_size', None)
    if token_size is not None:
        msg = f'{output_type} ({round(token_size/1048576, 1)} MB) has been extracted from {token}'
    else:
        msg = f'{output_type} has been extracted from {token}'
    if device:
        msg += f' using {device}'
    if not task_result.get('fresh', True):
        msg += ' (already done in the past)'
    if result_key:
        num_result, num_missing_token_status, num_active, num_fingerprinted = _count_active_and_fingerprinted()
        if num_result > 1:
            msg = str(num_result) + ' ' + msg
        if num_missing_token_status != num_result:
            if num_active != num_result:
                msg += f' ({num_active}/{num_result} are active)'
            else:
                msg += ' (all are active)'
            if num_fingerprinted != num_result:
                msg += f' ({num_fingerprinted}/{num_result} are fingerprinted)'
            else:
                msg += ' (all are fingerprinted)'
        if not quiet:
            status_msg(msg, color='green', sections=sections + ['SUCCESS'])
    else:
        if not quiet:
            status_msg(msg, color='green', sections=sections + ['SUCCESS'])


def _get_response(
        url: str, login_info: Dict[str, str], request_func: Callable = get, headers: Optional[Dict[str, str]] = None,
        json: Optional[Dict] = None, data: Optional[Dict] = None, max_tries=5,
        sections=tuple(), debug=False, delay_retry=1, timeout=600
) -> Optional[Response]:
    request_type = request_func.__name__.upper()
    if not url.startswith('http'):
        url = login_info['host'] + url
    if 'token' in login_info:
        if headers is None:
            headers = {"Authorization": f"Bearer {login_info['token']}"}
        else:
            headers["Authorization"] = f"Bearer {login_info['token']}"
    # wait for the response
    tries = 1
    while tries <= max_tries:
        if debug:
            msg = f'Sending {request_type} request to {url}'
            # if headers is not None:
            #     msg += f' with headers "{headers}"'
            if json is not None:
                msg += f' with json data "{json}"'
            print(msg)
        try:
            response = request_func(url, headers=headers, json=json, data=data, timeout=timeout)
        except Exception as e:
            if tries >= max_tries:
                raise e
            msg = f'Caught exception "{str(e)}" while doing {request_type} on {url} on try {tries}/{max_tries}'
            # if headers is not None:
            #     msg += f' with headers "{headers}"'
            if json is not None:
                msg += f' with json data "{json}"'
            status_msg(msg, color='yellow', sections=list(sections) + ['WARNING'])
            tries += 1
            sleep(delay_retry)
            continue
        status_code = response.status_code
        reason = response.reason
        if debug:
            print(f'Got response with code{status_code}: {response.text}')
        if response.ok:
            return response
        elif status_code == 401:
            status_msg(
                f'Error {status_code}: {reason}, trying to reconnect...',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            new_token = login(login_info['graph_api_json'])['token']
            login_info['token'] = new_token
            headers["Authorization"] = f"Bearer {new_token}"
            tries += 1
        else:
            status_msg(
                f'Error {status_code}: {reason} while doing {request_type} on {url}',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            if status_code == 422:
                response_json = response.json()
                if 'detail' in response_json:
                    if isinstance(response_json['detail'], list):
                        for detail in response_json['detail']:
                            status_msg(str(detail), color='yellow', sections=list(sections) + ['WARNING'])
                    else:
                        status_msg(str(response_json['detail']), color='yellow', sections=list(sections) + ['WARNING'])
            if status_code == 500:
                uuid_regexp = r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})'
                url_status_match = fullmatch(r'(:?https?://)?(.*)/status/' + uuid_regexp + r'/?', url)
                if url_status_match:
                    protocol, endpoint, task_uuid = url_status_match.groups()
                    flower_api_response = get('http://127.0.0.1:5555/api/task/info/' + task_uuid, timeout=10)
                    task_info = flower_api_response.json()
                else:
                    # we get info about the latest failed task
                    tasks_response = get('http://127.0.0.1:5555/api/tasks', timeout=30)
                    failed_tasks = [t for t_uuid, t in tasks_response.json().items() if t['state'] == 'FAILURE']
                    tasks_info = sorted(failed_tasks, key=lambda t: t['timestamp'], reverse=True)
                    if len(tasks_info) > 0:
                        task_info = tasks_info[0]
                    else:
                        task_info = None
                if task_info:
                    msg = f'Task {task_info["uuid"]} is in state {task_info["state"]} '
                    if task_info['exception']:
                        msg += f'with exception "{task_info["exception"]}" '
                    if task_info['traceback']:
                        msg += f'with traceback:\n{task_info["traceback"]}'
                else:
                    msg = 'No task info found.'
                status_msg(msg, color='yellow', sections=list(sections) + ['WARNING'])
            tries += 1
            sleep(delay_retry)
    msg = f'Maximum try {max_tries}/{max_tries} reached while doing {request_type} on "{url}"'
    # if headers is not None:
    #     msg += f' with headers "{headers}"'
    if json is not None:
        msg += f' with json data "{json}"'
    raise RuntimeError(msg)


def login(graph_api_json=None, max_tries=5):
    if graph_api_json is None:
        import graphai_client
        graph_api_json = normpath(join(dirname(graphai_client.__file__), 'config', 'graphai-api.json'))
    with open(graph_api_json) as fp:
        piper_con_info = load_json(fp)
    host_with_port = piper_con_info['host'] + ':' + str(piper_con_info['port'])
    login_info = {
        'user': piper_con_info['user'],
        'host': host_with_port,
        'graph_api_json': graph_api_json
    }
    response_login = _get_response(
        '/token', login_info, post, data={'username': piper_con_info['user'], 'password': piper_con_info['password']},
        max_tries=max_tries
    )
    login_info['token'] = response_login.json()['access_token']
    return login_info


def status_msg(msg, color=None, sections=(), print_flag=True):
    """
    Print a nice status message.

    :param msg: message to print.
    :type msg: str
    :param color: color of the message. If None, the default color is used. Available colors are:

        - 'grey', 'black', 'red', 'green', 'orange', 'blue', 'magenta', 'cyan', 'light gray', 'dark gray', 'light red',
            'light green', 'yellow', 'light purple', 'light cyan' and 'white' in terminal mode.
        - 'grey', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan' and 'white' in non-terminal mode.

    :type color: str, None
    :param sections: list of strings representing the sections which will be displayed at the beginning of the message.
    :type sections: Iterable[str]
    :param print_flag: If False nothing is printed.
    :type print_flag: bool
    """
    if not print_flag:
        return
    global_string = '[%s] ' % f"{datetime.now():%Y-%m-%d %H:%M:%S}"
    for section in sections:
        global_string += '[%s] ' % section
    global_string += msg
    cprint(global_string, color)


def get_google_api_credentials(service_name='youtube', google_api_json=None) -> dict:
    if google_api_json is None:
        import graphai_client
        path_config = join(dirname(graphai_client.__file__), 'config')
        google_api_json = join(path_config, f'google-api-{service_name}.json')
        if not exists(google_api_json):
            google_api_json = join(path_config, 'google-api.json')
        if not exists(google_api_json):
            raise RuntimeError(
                f'google_api_json is not set and no '
                f'google-api.json nor google-api-{service_name}.json file found in {path_config}.'
            )
    with open(google_api_json) as fp:
        api_credentials = load_json(fp)
    return api_credentials
