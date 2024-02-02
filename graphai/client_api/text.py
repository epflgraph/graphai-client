from requests import post, Session
from urllib.parse import urlencode
from typing import List
from graphai.client_api.utils import status_msg


def extract_concepts_from_text(
        text: str, restrict_to_ontology=False, graph_score_smoothing=True, ontology_score_smoothing=True,
        keywords_score_smoothing=True, normalisation_coefficient=0.5, filtering_threshold=0.1, filtering_min_votes=5,
        refresh_scores=True, graph_ai_server='http://127.0.0.1:28800', sections=('GRAPHAI', 'CONCEPT DETECTION'),
        debug=False, n_trials=5, session: Session = None
):
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
    url = graph_ai_server + '/text/wikify?' + urlencode(url_params)
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
            response = session.post(url, json=json)
            if debug:
                print(f'Got response with code{response.status_code}: {response.text}')
            if response.ok:
                if close_session:
                    session.close()
                return response.json()
            else:
                status_code = response.status_code
                msg =  f'Error {status_code}: {response.reason} while doing POST on {url}'
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
