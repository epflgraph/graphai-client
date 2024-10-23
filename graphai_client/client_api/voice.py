from typing import Optional, Tuple, List
from graphai_client.client_api.utils import call_async_endpoint, status_msg


def transcribe_audio(
        audio_token: str, login_info: dict, force=False, force_lang=None, sections=('GRAPHAI', 'TRANSCRIBE'),
        debug=False, strict=False, max_tries=5, max_processing_time_s=7200
) -> Tuple[Optional[str], Optional[List[dict]]]:
    """
    Transcribe the voices in the audio into segments (subtitles as a list).
    Srt file can be created from segment using graphai_client.utils.create_srt_file_from_segments()

    :param audio_token: audio token, typically returned by graphai.client_api.video.extract_audio()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the audio transcription forced.
    :param force_lang: if not None, the language of the transcription is forced to the given value
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param strict: if True, a more aggressive silence detection is applied.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to transcribe the voices from the audio.
    :return: a tuple where the first element is either the "force_lang" parameter if not None or the detected language
        of the audio or None if no voice was detected. The second element  of the tuple is a list of segments.
        Each segment is a dictionary with the start and end timestamp (in second) with resp. 'start' and 'end key',
        and the detected language as key and the detected text  during that interval as value.
    """
    json_data = {"token": audio_token, "force": force, "strict": strict}
    output_type = 'transcription'
    if force_lang is not None:
        json_data["force_lang"] = force_lang
        output_type = force_lang + ' ' + output_type
    task_result = call_async_endpoint(
        endpoint='/voice/transcribe',
        json=json_data,
        login_info=login_info,
        token=audio_token,
        output_type=output_type,
        result_key='subtitle_results',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None, None
    if task_result['subtitle_results'] is None:
        segments = None
        status_msg(
            f'No transcription been extracted from {audio_token}',
            color='yellow', sections=list(sections) + ['WARNING']
        )
    else:
        segments = [
            {'start': segment['start'], 'end': segment['end'], task_result['language']: segment['text'].strip()}
            for segment in task_result['subtitle_results']
        ]
    return task_result['language'], segments


def detect_language(
        audio_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'AUDIO LANGUAGE'), debug=False,
        max_tries=5, max_processing_time_s=3600
) -> Optional[str]:
    """
    Detect the language of the voice in the audio.

    :param audio_token: audio token, typically returned by graphai.client_api.video.extract_audio()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the language detection forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to detect the language from the voices in the audio.
    :return: the language  of the voice detected in the audio if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/voice/detect_language',
        json={"token": audio_token, "force": force},
        login_info=login_info,
        token=audio_token,
        output_type='language detection',
        result_key='language',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    if task_result['language'] is None:
        status_msg(
            f'Language could not be detected from {audio_token}',
            color='yellow', sections=list(sections) + ['WARNING']
        )
    else:
        status_msg(
            f'{task_result["language"]} language has been detected for audio {audio_token}',
            color='green', sections=list(sections) + ['SUCCESS']
        )
    return task_result['language']


def calculate_fingerprint(
        audio_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'AUDIO FINGERPRINT'), debug=False,
        max_tries=5, max_processing_time_s=300
):
    """
    Calculate the fingerprint of a single slide

    :param audio_token: audio token, typically returned by graphai.client_api.video.extract_audio()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the audio extraction forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param max_tries: the number of tries before giving up.
    :param max_processing_time_s: maximum number of seconds to perform the text extraction.
    :return: the fingerprint of the video if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/voice/calculate_fingerprint',
        json={"token": audio_token, "force": force},
        login_info=login_info,
        token=audio_token,
        output_type='fingerprint',
        result_key='result',
        max_tries=max_tries,
        max_processing_time_s=max_processing_time_s,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None
    return task_result['result']
