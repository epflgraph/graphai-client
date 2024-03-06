from json import load as load_json
from os.path import normpath, join, dirname
from time import sleep
from requests import get, post
from typing import Union
from graphai_client.utils import status_msg


def call_async_endpoint(
        endpoint, json, login_info, token, output_type, try_count=0,
        n_try=6000, delay_retry=1, sections=(), debug=False, quiet=False
):
    response_endpoint = _get_response(
        url=endpoint,
        login_info=login_info,
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json=json,
        sections=sections,
        debug=debug
    )
    if response_endpoint is None:
        return None
    # get task_id to poll for result
    task_id = response_endpoint.json()['task_id']
    # wait for the task to be completed
    while try_count < n_try:
        try_count += 1
        response_status = _get_response(
            url=f'{endpoint}/status/{task_id}',
            login_info=login_info,
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections,
            debug=debug
        )
        if response_status is None:
            return None
        response_status_json = response_status.json()
        task_status = response_status_json['task_status']
        if task_status in ['PENDING', 'STARTED']:
            sleep(delay_retry)
        elif task_status == 'SUCCESS':
            task_result = response_status_json['task_result']
            if not task_result_is_ok(task_result, token=token, output_type=output_type, sections=sections, quiet=quiet):
                sleep(delay_retry)
                continue
            return task_result
        elif task_status == 'FAILURE':
            status_msg(
                f'Calling {endpoint} caused a failure. The response was:\n{response_status_json}'
                f'\nThe data was:\n{json}',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            return None
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of {endpoint} for {token}: '
                + task_status
            )
    status_msg(
        f'Maximum trials reached for {endpoint} with the following json data: \n{json}',
        color='yellow', sections=list(sections) + ['WARNING']
    )
    return None


def _get_response(
        url: str, login_info, request_func=get, headers=None, json=None, data=None, n_trials=5, sections=tuple(),
        debug=False, delay_retry=1
):
    trials = 0
    status_code = None
    reason = None
    request_type = request_func.__name__.upper()
    if not url.startswith('http'):
        url = login_info['host'] + url
    if 'token' in login_info:
        if headers is None:
            headers = {"Authorization": f"Bearer {login_info['token']}"}
        else:
            headers["Authorization"] = f"Bearer {login_info['token']}"
    while trials < n_trials:
        trials += 1
        if debug:
            msg = f'Sending {request_type} request to {url}'
            if headers is not None:
                msg += f' with headers "{headers}"'
            if json is not None:
                msg += f' with json data "{json}"'
            print(msg)
        try:
            response = request_func(url, headers=headers, json=json, data=data)
        except Exception as e:
            if trials == n_trials:
                raise e
            msg = f'Caught exception "{str(e)}" while doing {request_type} on {url}'
            if headers is not None:
                msg += f' with headers "{headers}"'
            if json is not None:
                msg += f' with json data "{json}"'
            status_msg(msg, color='yellow', sections=list(sections) + ['WARNING'])
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
                f'Error {status_code}: {response.reason}, trying to reconnect...',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            new_token = login(login_info['graph_api_json'])['token']
            login_info['token'] = new_token
            headers["Authorization"] = f"Bearer {new_token}"
        else:
            status_msg(
                f'Error {status_code}: {response.reason} while doing {request_type} on {url}',
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
            sleep(delay_retry)
    msg = f'Error {status_code}: {reason} while doing {request_type} on "{url}"'
    if headers is not None:
        msg += f' with headers "{headers}"'
    if json is not None:
        msg += f' with json data "{json}"'
    raise RuntimeError(msg)


def task_result_is_ok(task_result: Union[dict, None], token: str, output_type='text', sections=tuple(), quiet=False):
    if task_result is None:
        status_msg(
            f'Bad task result while extracting {output_type} from {token}',
            color='yellow', sections=list(sections) + ['WARNING']
        )
        return False
    if not task_result.get('successful', True):
        status_msg(
            f'extraction of the {output_type} from {token} failed',
            color='yellow', sections=list(sections) + ['WARNING']
        )
        return False
    if not quiet:
        if not task_result.get('fresh', True):
            status_msg(
                f'{output_type} from {token} has already been extracted in the past',
                color='yellow', sections=list(sections) + ['WARNING']
            )
        else:
            status_msg(
                f'{output_type} has been extracted from {token}',
                color='green', sections=list(sections) + ['SUCCESS']
            )
    return True


def login(graph_api_json=None):
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
        '/token', login_info, post, data={'username': piper_con_info['user'], 'password': piper_con_info['password']}
    )
    login_info['token'] = response_login.json()['access_token']
    return login_info
