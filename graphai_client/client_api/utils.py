from json import load as load_json
from os.path import normpath, join, dirname
from time import sleep
from datetime import datetime, timedelta
from requests import get, post, Response
from typing import Callable, Dict, Optional, Tuple
from graphai_client.utils import status_msg


def call_async_endpoint(
        endpoint, json, login_info, token, output_type, result_key=None, max_processing_time_s=6000,
        max_tries=5, delay_retry=1, sections=(), debug=False, quiet=False, _tries=1,
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
    :param result_key: key of the main result, this parameter is only used for logs.
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
        max_tries=5,
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
            max_tries=5,
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
                    f'Got unexpected response_status: {response_status_json} while extracting {output_type} from {token} '
                    f'at try {_tries}/{max_tries}', color='yellow', sections=list(sections) + ['WARNING']
                )
            _tries += 1
            sleep(delay_retry)
            continue
        task_status = response_status_json['task_status']
        if task_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif task_status == 'SUCCESS':
            task_result = response_status_json.get('task_result', None)
            if task_result is None or not isinstance(task_result, dict):
                if not quiet or _tries == max_tries:
                    status_msg(
                        f'Bad task result "{task_result}" while extracting {output_type} from {token} '
                        f'at try {_tries}/{max_tries}',
                        color='yellow', sections=list(sections) + ['WARNING']
                    )
                _tries += 1
                sleep(delay_retry)
                continue
            if not task_result.get('successful', True):
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
            _check_result(task_result, result_key, token, output_type, list(sections), quiet)
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


def _check_result(
        task_result: dict, result_key: str, token: str, output_type: str, sections: list, quiet: bool
) -> None:
    def _is_active_and_fingerprinted(task_res: dict, output_id: str) -> Tuple[Optional[bool], Optional[bool]]:
        if not isinstance(task_res, dict):
            raise RuntimeError(f'invalid result type for {output_id}')
        token_status = task_res.get('token_status', None)
        if token_status is None:
            return None, None
        if isinstance(token_status, dict):
            return token_status.get('active', False), token_status.get('fingerprinted', False)
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
                is_active, is_fp = _is_active_and_fingerprinted(res, f'{output_type} {key} from {token}')
                if is_active is None and is_fp is None:
                    n_missing_token_status += 1
                if is_active:
                    n_active += 1
                if is_fp:
                    n_fingerprinted += 1
        elif isinstance(result, list) or isinstance(result, tuple):
            for idx, res in enumerate(result):
                is_active, is_fp = _is_active_and_fingerprinted(res, f'{output_type} {idx} from {token}')
                if is_active is None and is_fp is None:
                    n_missing_token_status += 1
                if is_active:
                    n_active += 1
                if is_fp:
                    n_fingerprinted += 1
        else:
            is_active, is_fp = _is_active_and_fingerprinted(task_result, f'{output_type} from {token}')
            if is_active is None and is_fp is None:
                n_missing_token_status += 1
            if is_active:
                n_active += 1
            if is_fp:
                n_fingerprinted += 1
        return n_result, n_missing_token_status, n_active, n_fingerprinted

    num_result = 1
    if result_key:
        num_result, num_missing_token_status, num_active, num_fingerprinted = _count_active_and_fingerprinted()
        msg = f'{num_result} {output_type} has been extracted from {token}'
        if not task_result.get('fresh', True):
            msg += f' (already done in the past)'
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
            status_msg(
                f'{num_result} {output_type} has been extracted from {token}',
                color='green', sections=sections + ['SUCCESS']
            )


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
