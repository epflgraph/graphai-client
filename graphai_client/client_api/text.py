from requests import Session
from urllib.parse import urlencode
from graphai_client.client_api import login
from graphai_client.client_api.utils import status_msg


def extract_concepts_from_text(
        text: str, login_info: dict, restrict_to_ontology=False, graph_score_smoothing=True,
        ontology_score_smoothing=True, keywords_score_smoothing=True, normalisation_coefficient=0.5,
        filtering_threshold=0.1, filtering_min_votes=5, refresh_scores=True, sections=('GRAPHAI', 'CONCEPT DETECTION'),
        debug=False, n_trials=5, session: Session = None
):
    if 'token' not in login_info:
        login_info = login(login_info['graph_api_json'])
    close_session = False
    if session is None:
        session = Session()
        close_session = True
    url_params = dict(
        restrict_to_ontology=restrict_to_ontology,
        graph_score_smoothing=graph_score_smoothing,
        ontology_score_smoothing=ontology_score_smoothing,
        keywords_score_smoothing=keywords_score_smoothing,
        normalisation_coef=normalisation_coefficient,
        filtering_threshold=filtering_threshold,
        filtering_min_votes=filtering_min_votes,
        refresh_scores=refresh_scores
    )
    url = login_info['host'] + '/text/wikify?' + urlencode(url_params)
    json = {'raw_text': text}
    status_code = None
    trials = 0
    while trials < n_trials:
        trials += 1
        if debug:
            msg = f'Sending post request to {url}'
            if json is not None:
                msg += f' with json data "{json}"'
            print(msg)
        try:
            response = session.post(url, json=json, headers={"Authorization": f"Bearer {login_info['token']}"})
            status_code = response.status_code
            if debug:
                print(f'Got response with code{response.status_code}: {response.text}')
            if response.ok:
                if close_session:
                    session.close()
                return response.json()
            elif status_code == '401':
                status_msg(
                    f'Error {status_code}: {response.reason}, trying to reconnect...',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
                login_info = login(login_info['graph_api_json'])
            else:
                msg = f'Error {status_code}: {response.reason} while doing POST on {url}'
                if json is not None:
                    msg += f' with json data "{json}"'
                status_msg(msg, color='yellow', sections=list(sections) + ['WARNING'])
        except Exception as e:
            msg = f'Caught exception "{str(e)}" while doing POST on {url}'
            if json is not None:
                msg += f' with json data "{json}"'
            status_msg(msg, color='yellow', sections=list(sections) + ['WARNING'])
    if status_code == 500:
        msg = f'Could not get response for POST on "{url}"'
        if json is not None:
            msg += f' with json data "{json}"'
    if close_session:
        session.close()
    return None
