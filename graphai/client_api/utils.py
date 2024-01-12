from time import sleep
from requests import get
from typing import Union
from graphai.utils import status_msg


def get_response(url, request_func=get, headers=None, json=None, n_trials=5, sections=tuple(), debug=False):
    trials = 0
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
        if debug:
            print(f'Got response with code{response.status_code}: {response.text}')
        if response.ok:
            return response
        else:
            status_msg(
                f'Error {response.status_code}: {response.reason} while doing {request_func.__name__.upper()} on {url}',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            if response.status_code == 422:
                response_json = response.json()
                if 'detail' in response_json:
                    if isinstance(response_json['detail'], list):
                        for detail in response_json['detail']:
                            status_msg(str(detail), color='yellow', sections=list(sections) + ['WARNING'])
                    status_msg(str(response_json['detail']), color='yellow', sections=list(sections) + ['WARNING'])
            sleep(1)
    if response.status_code == 500:
        raise RuntimeError(f'could not get response for {request_func.__name__.upper()} on "{url}"')
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
