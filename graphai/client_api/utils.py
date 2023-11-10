from time import sleep
from requests import get
from typing import Union
from graphai.utils import StatusMSG


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
            StatusMSG(
                f'Error {response.status_code}: {response.reason} while doing {request_func.__name__.upper()} on {url}',
                Color='yellow', Sections=list(sections) + ['WARNING']
            )
            if response.status_code == 422:
                response_json = response.json()
                if 'detail' in response_json:
                    if isinstance(response_json['detail'], list):
                        for detail in response_json['detail']:
                            StatusMSG(str(detail), Color='yellow', Sections=list(sections) + ['WARNING'])
                    StatusMSG(str(response_json['detail']), Color='yellow', Sections=list(sections) + ['WARNING'])
            sleep(1)
    return None


def task_result_is_ok(task_result: Union[dict, None], token: str, input_type='text', sections=('GRAPHAI', 'OCR')):
    if task_result is None:
        StatusMSG(
            f'Bad task result while extracting {input_type} from {token}',
            Color='yellow', Sections=list(sections) + ['WARNING']
        )
        return False
    if not task_result['successful']:
        StatusMSG(
            f'extraction of the {input_type} from {token} failed',
            Color='yellow', Sections=list(sections) + ['WARNING']
        )
        return False
    if not task_result['fresh']:
        StatusMSG(
            f'{input_type} from {token} has already been extracted in the past',
            Color='yellow', Sections=list(sections) + ['WARNING']
        )
    else:
        StatusMSG(
            f'{input_type} has been extracted from {token}',
            Color='green', Sections=list(sections) + ['SUCCESS']
        )
    return True
