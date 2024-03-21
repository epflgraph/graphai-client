from typing import Optional, Tuple, List
from graphai_client.client_api.utils import call_async_endpoint
from graphai_client.utils import status_msg


def transcribe_audio(
        audio_token: str, login_info: dict, force=False, force_lang=None, sections=('GRAPHAI', 'TRANSCRIBE'),
        debug=False, strict=False, n_try=6000, delay_retry=1
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
    :param n_try: the number of tries before giving up.
    :param delay_retry: the time to wait between tries.
    :return: a tuple where the first element is either the "force_lang" parameter if not None or the detected language
        of the audio or None if no voice was detected). The second element  of the tuple is a list of segments.
        Each segment is a dictionary with the start and end timestamp (in second) with resp. 'start' and 'end key',
        and the detected language as key and the detected text  during that interval as value.
    """
    json_data = {"token": audio_token, "force": force, "strict": strict}
    if force_lang is not None:
        json_data["force_lang"] = force_lang
    task_result = call_async_endpoint(
        endpoint='/voice/transcribe',
        json=json_data,
        login_info=login_info,
        token=audio_token,
        output_type='transcription',
        n_try=n_try,
        delay_retry=delay_retry,
        sections=sections,
        debug=debug
    )
    if task_result is None:
        return None, None
    if task_result['subtitle_results'] is None:
        segments = None
        status_msg(
            f'No segments have been extracted from {audio_token}',
            color='yellow', sections=list(sections) + ['WARNING']
        )
    else:
        segments = [
            {'start': segment['start'], 'end': segment['end'], task_result['language']: segment['text'].strip()}
            for segment in task_result['subtitle_results']
        ]
        status_msg(
            f'{len(segments)} segments have been extracted from {audio_token}',
            color='green', sections=list(sections) + ['SUCCESS']
        )
    return task_result['language'], segments


def detect_language(
        audio_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'AUDIO LANGUAGE'), debug=False,
        n_try=6000, delay_retry=1
) -> Optional[str]:
    """
    Detect the language of the voice in the audio.

    :param audio_token: audio token, typically returned by graphai.client_api.video.extract_audio()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param force: Should the cache be bypassed and the language detection forced.
    :param sections: sections to use in the status messages.
    :param debug: if True additional information about each connection to the API is displayed.
    :param n_try: the number of tries before giving up.
    :param delay_retry: the time to wait between tries.
    :return: the language  of the voice detected in the audio if successful, None otherwise.
    """
    task_result = call_async_endpoint(
        endpoint='/voice/detect_language',
        json={"token": audio_token, "force": force},
        login_info=login_info,
        token=audio_token,
        output_type='language detection',
        n_try=n_try,
        delay_retry=delay_retry,
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

