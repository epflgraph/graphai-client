from graphai_client.client_api.utils import get_response
from time import sleep
from requests import get, post
from graphai_client.utils import status_msg


def transcribe_audio(
        audio_token: str, login_info: dict, force=False, force_lang=None, sections=('GRAPHAI', 'TRANSCRIBE'),
        debug=False, strict=False
):
    json_data = {"token": audio_token, "force": force, "strict": strict}
    if force_lang is not None:
        json_data["force_lang"] = force_lang
    response_transcribe = get_response(
        url='/voice/transcribe',
        login_info=login_info,
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json=json_data,
        sections=sections,
        debug=debug
    )
    if response_transcribe is None:
        return force_lang, None
    task_id = response_transcribe.json()['task_id']
    # wait for the transcription to be completed
    tries_transcribe_status = 0
    while tries_transcribe_status < 6000:
        tries_transcribe_status += 1
        response_transcribe_status = get_response(
            url=f'/voice/transcribe/status/{task_id}',
            login_info=login_info,
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections,
            debug=debug
        )
        if response_transcribe_status is None:
            return force_lang, None
        response_transcribe_status_json = response_transcribe_status.json()
        transcribe_status = response_transcribe_status_json['task_status']
        if transcribe_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif transcribe_status == 'SUCCESS':
            task_result = response_transcribe_status_json['task_result']
            if task_result is None:
                sleep(1)
                continue
            if not task_result['fresh']:
                status_msg(
                    f'audio {audio_token} has already been transcribed in the past',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
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
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of transcription for {audio_token}: '
                + transcribe_status
            )


def detect_language(
        audio_token: str, login_info: dict, force=False, sections=('GRAPHAI', 'AUDIO LANGUAGE'), debug=False
):
    json_data = {"token": audio_token, "force": force}
    response_language = get_response(
        url='/voice/detect_language',
        login_info=login_info,
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json=json_data,
        sections=sections,
        debug=debug
    )
    if response_language is None:
        return None
    task_id = response_language.json()['task_id']
    # wait for the transcription to be completed
    tries_language_status = 0
    while tries_language_status < 6000:
        tries_language_status += 1
        response_language_status = get_response(
            url=f'/voice/detect_language/status/{task_id}',
            login_info=login_info,
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections,
            debug=debug
        )
        if response_language_status is None:
            return None
        response_language_status_json = response_language_status.json()
        language_status = response_language_status_json['task_status']
        if language_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif language_status == 'SUCCESS':
            task_result = response_language_status_json['task_result']
            if task_result is None:
                sleep(1)
                continue
            if not task_result['fresh']:
                status_msg(
                    f'language of audio {audio_token} has already been detected in the past',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
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
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of language detection for {audio_token}: '
                + language_status
            )
