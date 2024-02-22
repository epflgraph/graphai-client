from time import sleep
from requests import get
from typing import Union
from graphai_client.utils import status_msg
from graphai_client.client_api import login


def get_response(
        url: str, login_info: dict, request_func=get, headers=None, json=None, n_trials=5, sections=tuple(), debug=False
):
    trials = 0
    status_code = None
    if 'token' not in login_info:
        login_info = login(login_info['graph_api_json'])
    if not url.startswith('http'):
        url = login_info['host'] + url
    if headers is None:
        headers = {"Authorization": f"Bearer {login_info['token']}"}
    else:
        headers["Authorization"] = f"Bearer {login_info['token']}"
    while trials < n_trials:
        trials += 1
        if debug:
            msg = f'Sending {request_func.__name__.upper()} request to {url}'
            if headers is not None:
                msg += f' with headers "{headers}"'
            if json is not None:
                msg += f' with json data "{json}"'
            print(msg)
        response = request_func(url, headers=headers, json=json)
        status_code = response.status_code
        if debug:
            print(f'Got response with code{status_code}: {response.text}')
        if response.ok:
            return response
        elif status_code == '401':
            status_msg(
                f'Error {status_code}: {response.reason}, trying to reconnect...',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            login_info = login(login_info['graph_api_json'])
        else:
            status_msg(
                f'Error {status_code}: {response.reason} while doing {request_func.__name__.upper()} on {url}',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            if status_code == 422:
                response_json = response.json()
                if 'detail' in response_json:
                    if isinstance(response_json['detail'], list):
                        for detail in response_json['detail']:
                            status_msg(str(detail), color='yellow', sections=list(sections) + ['WARNING'])
                    status_msg(str(response_json['detail']), color='yellow', sections=list(sections) + ['WARNING'])
            sleep(1)
    if status_code == 500:
        msg = f'Could not get response for {request_func.__name__.upper()} on "{url}"'
        if headers is not None:
            msg += f' with headers "{headers}"'
        if json is not None:
            msg += f' with json data "{json}"'
        raise RuntimeError(msg)
    else:
        return None


def task_result_is_ok(task_result: Union[dict, None], token: str, input_type='text', sections=('GRAPHAI', 'OCR')):
    if task_result is None:
        status_msg(
            f'Bad task result while extracting {input_type} from {token}',
            color='yellow', sections=list(sections) + ['WARNING']
        )
        return False
    if not task_result['successful']:
        status_msg(
            f'extraction of the {input_type} from {token} failed',
            color='yellow', sections=list(sections) + ['WARNING']
        )
        return False
    if not task_result['fresh']:
        status_msg(
            f'{input_type} from {token} has already been extracted in the past',
            color='yellow', sections=list(sections) + ['WARNING']
        )
    else:
        status_msg(
            f'{input_type} has been extracted from {token}',
            color='green', sections=list(sections) + ['SUCCESS']
        )
    return True


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
