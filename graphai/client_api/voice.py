from graphai.client_api.utils import get_response
from time import sleep
from requests import get, post
from graphai.utils import StatusMSG


def transcribe_audio(
        audio_token, force=False, force_lang=None, graph_ai_server='http://127.0.0.1:28800',
        sections=('GRAPHAI', 'TRANSCRIBE'), debug=False
):
    json_data = {"token": audio_token, "force": force}
    if force_lang is not None:
        json_data["force_lang"] = force_lang
    response_transcribe = get_response(
        url=graph_ai_server + '/voice/transcribe',
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
            url=graph_ai_server + f'/voice/transcribe/status/{task_id}',
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
            if not task_result['fresh']:
                StatusMSG(
                    f'audio {audio_token} has already been transcribed in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            segments = [
                {'start': segment['start'], 'end': segment['end'], task_result['language']: segment['text'].strip()}
                for segment in task_result['subtitle_results']
            ]
            StatusMSG(
                f'{len(segments)} segments have been extracted from {audio_token}',
                Color='green', Sections=list(sections) + ['SUCCESS']
            )
            return task_result['language'], segments
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of transcription for {audio_token}: '
                + transcribe_status
            )
