from os.path import join, dirname, normpath
from json import load as load_json
from requests import post


def login(graph_api_json=None):
    if graph_api_json is None:
        import graphai_client
        graph_api_json = normpath(join(dirname(graphai_client.__file__), 'config', 'graphai-api.json'))
    with open(graph_api_json) as fp:
        piper_con_info = load_json(fp)
    host_with_port = piper_con_info['host'] + ':' + str(piper_con_info['port'])
    response_login = post(
        host_with_port + '/token',
        data={'username': piper_con_info['user'], 'password': piper_con_info['password']}, timeout=10
    )
    response_login.raise_for_status()
    token = response_login.json()['access_token']
    login_info = {
        'user': piper_con_info['user'],
        'host': host_with_port,
        'token': token,
        'graph_api_json': graph_api_json
    }
    return login_info
