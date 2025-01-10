from requests import Session, post
from urllib.parse import urlencode
from unicodedata import normalize
from typing import Optional, List, Tuple
from graphai_client.client_api.utils import _get_response, login, status_msg
from graphai_client.client_api.translation import detect_language, translate_text


def extract_concepts_from_text(
        text: str, login_info: dict, restrict_to_ontology=False, graph_score_smoothing=True,
        ontology_score_smoothing=True, keywords_score_smoothing=True, normalisation_coefficient=0.5,
        filtering_threshold=0.1, filtering_min_votes=5, refresh_scores=True, sections=('GRAPHAI', 'CONCEPT DETECTION'),
        debug=False, max_tries=15, delay_retry=60, session: Optional[Session] = None
) -> Optional[List[dict]]:
    """
    Detect concepts (wikipedia pages) with associated scores from an input text.
    :param text: text to analyze
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json)
    :param restrict_to_ontology: refer to the API documentation
    :param graph_score_smoothing: refer to the API documentation
    :param ontology_score_smoothing: refer to the API documentation
    :param keywords_score_smoothing: refer to the API documentation
    :param normalisation_coefficient: refer to the API documentation
    :param filtering_threshold: if a concept has a mixed score less than this threshold, it is discarded.
    :param filtering_min_votes: if a concept is related to fewer keywords than this threshold, it is discarded.
    :param refresh_scores: recompute scores after filters have been applied.
        It can be safely set to False if filtering_threshold=0 and filtering_min_votes=0.
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


def extract_keywords_from_text(
        text: str, login_info: dict, use_nltk=False, sections=('GRAPHAI', 'CONCEPT DETECTION'),
        debug=False, max_tries=15, delay_retry=60, session: Optional[Session] = None
) -> Optional[List[str]]:
    """
    Extract keywords from an input text.
    :param text: text to analyze
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json)
    :param use_nltk: refer to the API documentation
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_tries: number of trials to perform in case of errors before giving up.
    :param delay_retry: the time to wait between tries.
    :param session: optional requests.Session object.
    :return: a list of keywords if successful, None otherwise.
    """
    if 'token' not in login_info:
        login_info = login(login_info['graph_api_json'])
    if session is None:
        request_func = post
    else:
        request_func = session.post
    url_params = dict(use_nltk=use_nltk)
    url = login_info['host'] + '/text/keywords?' + urlencode(url_params)
    json = {'raw_text': text}
    response = _get_response(
        url, login_info, request_func=request_func, json=json, max_tries=max_tries, sections=sections, debug=debug,
        delay_retry=delay_retry, timeout=900
    )
    if response is None:
        return None
    return response.json()


def extract_concepts_from_keywords(
        keywords: List[str], login_info: dict, restrict_to_ontology=False, graph_score_smoothing=True,
        ontology_score_smoothing=True, keywords_score_smoothing=True, normalisation_coefficient=0.5,
        filtering_threshold=0.1, filtering_min_votes=5, refresh_scores=True, sections=('GRAPHAI', 'CONCEPT DETECTION'),
        debug=False, max_tries=15, delay_retry=60, session: Optional[Session] = None
) -> Optional[List[dict]]:
    """
    Detect concepts (wikipedia pages) with associated scores from a list of keywords.
    :param keywords: list of keywords to analyze.
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json)
    :param restrict_to_ontology: refer to the API documentation
    :param graph_score_smoothing: refer to the API documentation
    :param ontology_score_smoothing: refer to the API documentation
    :param keywords_score_smoothing: refer to the API documentation
    :param normalisation_coefficient: refer to the API documentation
    :param filtering_threshold: if a concept has a mixed score less than this threshold, it is discarded.
    :param filtering_min_votes: if a concept is related to fewer keywords than this threshold, it is discarded.
    :param refresh_scores: recompute scores after filters have been applied.
        It can be safely set to False if filtering_threshold=0 and filtering_min_votes=0.
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
    json = {'keywords': keywords}
    response = _get_response(
        url, login_info, request_func=request_func, json=json, max_tries=max_tries, sections=sections, debug=debug,
        delay_retry=delay_retry, timeout=900
    )
    if response is None:
        return None
    return response.json()


def clean_text_translate_extract_keywords_and_concepts(
        text_data: Tuple, login_info, additional_keywords: Optional[List[str]] = None,
        max_tries=15, delay_retry=60, translate_to_en=False,
        restrict_to_ontology=False, graph_score_smoothing=True,
        ontology_score_smoothing=True, keywords_score_smoothing=True, normalisation_coefficient=0.5,
        filtering_threshold=0.1, filtering_min_votes=5, refresh_scores=True,
        sections=('GRAPHAI', 'CONCEPT DETECTION',), session: Optional[Session] = None
):
    text = '. '.join([str(x) for x in text_data if x is not None])
    sections = list(sections)
    if not text:
        return None
    text_cleaned = normalize('NFKC', text)
    if translate_to_en and text_cleaned:
        lang_detected = detect_language(
            text_cleaned, login_info, sections=sections + ['DETECT LANGUAGE'], quiet=True, max_tries=max_tries,
            delay_retry=delay_retry
        )
        if lang_detected != 'en':
            if lang_detected not in ('fr', 'de', 'it'):
                status_msg(
                    f'unsupported language {lang_detected} detected from "{text_cleaned}"',
                    color='yellow', sections=sections + ['DETECT LANGUAGE', 'WARNING']
                )
            else:
                translated_text = translate_text(
                    text_cleaned, lang_detected, 'en', login_info, sections=sections + ['TRANSLATE'],
                    max_tries=max_tries, delay_retry=delay_retry
                )
                if translated_text:
                    text_cleaned = translated_text
    keywords = extract_keywords_from_text(
        text_cleaned, login_info, sections=sections + ['KEYWORDS EXTRACTION'], max_tries=max_tries,
        delay_retry=delay_retry, session=session
    )
    if not keywords:
        status_msg(
            'Got no keywords with default method. Trying nltk...',
            color='yellow', sections=sections + ['KEYWORDS EXTRACTION', 'WARNING']
        )
        keywords = extract_keywords_from_text(
            text_cleaned, login_info, sections=sections + ['KEYWORDS EXTRACTION'], max_tries=max_tries,
            delay_retry=delay_retry, session=session, use_nltk=True
        )
    if additional_keywords:
        keywords.extend(additional_keywords)
    if not keywords:
        msg = 'Error extracting keywords from text="' + text_cleaned + '".'
        status_msg(msg, color='yellow', sections=sections + ['KEYWORDS EXTRACTION', 'ERROR'])
        return None
    concepts_and_scores = extract_concepts_from_keywords(
        keywords, login_info, restrict_to_ontology=restrict_to_ontology, graph_score_smoothing=graph_score_smoothing,
        ontology_score_smoothing=ontology_score_smoothing, keywords_score_smoothing=keywords_score_smoothing,
        normalisation_coefficient=normalisation_coefficient, filtering_threshold=filtering_threshold,
        filtering_min_votes=filtering_min_votes, refresh_scores=refresh_scores,
        sections=sections, max_tries=max_tries, delay_retry=delay_retry, session=session
    )
    if not concepts_and_scores:
        msg = 'Error extracting concepts from keywords ' + ', '.join([f'"{w}"' for w in keywords]) + \
              ' while text was: "' + text_cleaned + '".'
        status_msg(msg, color='yellow', sections=sections + ['ERROR'])
    return {'keywords': keywords, 'concepts_and_scores': concepts_and_scores}
