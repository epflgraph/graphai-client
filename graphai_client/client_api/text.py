from requests import Session, post
from urllib.parse import urlencode
from typing import Optional
from graphai_client.client_api.utils import _get_response, login


def extract_concepts_from_text(
        text: str, login_info: dict, restrict_to_ontology=False, graph_score_smoothing=True,
        ontology_score_smoothing=True, keywords_score_smoothing=True, normalisation_coefficient=0.5,
        filtering_threshold=0.1, filtering_min_votes=5, refresh_scores=True, sections=('GRAPHAI', 'CONCEPT DETECTION'),
        debug=False, max_tries=15, delay_retry=60, session: Optional[Session] = None
):
    """
    Detect concepts (wikipedia pages) with associated scores from a input text.
    :param text: text to analyze
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json)
    :param restrict_to_ontology: refer to the API documentation
    :param graph_score_smoothing: refer to the API documentation
    :param ontology_score_smoothing: refer to the API documentation
    :param keywords_score_smoothing: refer to the API documentation
    :param normalisation_coefficient: refer to the API documentation
    :param filtering_threshold: refer to the API documentation
    :param filtering_min_votes: refer to the API documentation
    :param refresh_scores: refer to the API documentation
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_tries: number of trials to perform in case of errors before giving up.
    :param delay_retry: the time to wait between tries.
    :param session: optional requests.Session object.
    :return: a list of dictionary containing the concept and the associated scores if successful, None otherwise.
    """
    if 'token' not in login_info:
        login_info = login(login_info['graph_api_json'])
    if session is None:
        request_func = post
    else:
        request_func = session.post
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
    response = _get_response(
        url, login_info, request_func=request_func, json=json, max_tries=max_tries, sections=sections, debug=debug,
        delay_retry=delay_retry, timeout=900
    )
    if response is None:
        return None
    return response.json()
