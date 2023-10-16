from requests import get, post
from .utils import StatusMSG
from time import sleep


graph_ai_server = 'http://127.0.0.1:28800'  # port-forward to graphai-test


def get_response(url, request_func=get, headers=None, json=None, n_trials=5, sections=tuple()):
    trials = 0
    while trials < n_trials:
        trials += 1
        response = request_func(url, headers=headers, json=json)
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
                    StatusMSG(response_json['detail'], Color='yellow', Sections=list(sections) + ['WARNING'])
            sleep(1)
    return None


def get_video_token(url_video, playlist=False, sections=('GRAPHAI', 'DOWNLOAD VIDEO')):

    # retrieval of the video on graph-ai
    response_retrieve = get_response(
        url=graph_ai_server + '/video/retrieve_url',
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"url": url_video, "playlist": playlist},
        sections=sections
    )
    if response_retrieve is None:
        return None
    task_id = response_retrieve.json()['task_id']
    # wait for the retrieval to be completed
    tries_retrieve_status = 0
    while tries_retrieve_status < 6000:
        tries_retrieve_status += 1
        response_retrieve_status = get_response(
            url=graph_ai_server + f'/video/retrieve_url/status/{task_id}',
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections
        )
        if response_retrieve_status is None:
            return None
        response_retrieve_status_json = response_retrieve_status.json()
        retrieve_status = response_retrieve_status_json['task_status']
        if retrieve_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif retrieve_status == 'SUCCESS':
            task_result = response_retrieve_status_json['task_result']
            if not task_result['fresh']:
                StatusMSG(
                    f'{url_video} has already been retrieved in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'retrieving of {url_video} failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            return task_result['token']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of retrieve_url for {url_video}: ' + retrieve_status
            )


def fingerprint_video(video_token, force=False, sections=('GRAPHAI', 'FINGERPRINT VIDEO')):
    response_fingerprint = get_response(
        url=graph_ai_server + '/video/calculate_fingerprint',
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"token": video_token, "force": force},
        sections=sections
    )
    if response_fingerprint is None:
        return None
    task_id = response_fingerprint.json()['task_id']
    # wait for the fingerprinting to be completed
    tries_fingerprint_status = 0
    while tries_fingerprint_status < 6000:
        tries_fingerprint_status += 1
        response_fingerprint_status = get_response(
            url=graph_ai_server + f'/video/calculate_fingerprint/status/{task_id}',
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections
        )
        if response_fingerprint_status is None:
            return None
        response_fingerprint_status_json = response_fingerprint_status.json()
        fingerprint_status = response_fingerprint_status_json['task_status']
        if fingerprint_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif fingerprint_status == 'SUCCESS':
            task_result = response_fingerprint_status_json['task_result']
            if not task_result['fresh']:
                StatusMSG(
                    f'{video_token} has already been fingerprinted in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'fingerprinting of {video_token} failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            return task_result['result']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of fingerprinting for {video_token}: '
                + fingerprint_status
            )


def extract_audio(video_token, force=False, sections=('GRAPHAI', 'EXTRACT AUDIO')):
    response_extraction = get_response(
        url=graph_ai_server + '/video/extract_audio',
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"token": video_token, "force_non_self": True, "force": force},
        sections=sections
    )
    if response_extraction is None:
        return None
    task_id = response_extraction.json()['task_id']
    # wait for the extraction to be completed
    tries_extraction_status = 0
    while tries_extraction_status < 6000:
        tries_extraction_status += 1
        response_extraction_status = get_response(
            url=graph_ai_server + f'/video/extract_audio/status/{task_id}',
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections
        )
        if response_extraction_status is None:
            return None
        response_extraction_status_json = response_extraction_status.json()
        extraction_status = response_extraction_status_json['task_status']
        if extraction_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif extraction_status == 'SUCCESS':
            task_result = response_extraction_status_json['task_result']
            if not task_result['fresh']:
                StatusMSG(
                    f'audio from {video_token} has already been extracted in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'extraction of the audio from {video_token} failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            return task_result['token']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of audio extraction for {video_token}: '
                + extraction_status
            )


def extract_slides(video_token, force=False, sections=('GRAPHAI', 'EXTRACT SLIDES')):
    response_slides = get_response(
        url=graph_ai_server + '/video/detect_slides',
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"token": video_token, "force_non_self": False, "force": force},
        sections=sections
    )
    if response_slides is None:
        return None
    task_id = response_slides.json()['task_id']
    # wait for the detection of slides to be completed
    tries_slides_status = 0
    while tries_slides_status < 6000:
        tries_slides_status += 1
        response_slides_status = get_response(
            url=graph_ai_server + f'/video/detect_slides/status/{task_id}',
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections
        )
        if response_slides_status is None:
            return None
        response_slides_status_json = response_slides_status.json()
        slides_status = response_slides_status_json['task_status']
        if slides_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif slides_status == 'SUCCESS':
            task_result = response_slides_status_json['task_result']
            if not task_result['fresh']:
                StatusMSG(
                    f'slides from {video_token} has already been extracted in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'extraction of the slides from {video_token} failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            return task_result['slide_tokens']
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of slide extraction for {video_token}: '
                + slides_status
            )


def extract_text_from_slide(slide_token, force=False, sections=('GRAPHAI', 'OCR')):
    # extract text (using google OCR) from a single slide
    response_text = get_response(
        url=graph_ai_server + '/image/extract_text',
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"token": slide_token, "method": "google", "force": force},
        sections=sections
    )
    if response_text is None:
        return None
    task_id = response_text.json()['task_id']
    # wait for the detection of slides to be completed
    tries_text_status = 0
    while tries_text_status < 6000:
        tries_text_status += 1
        response_text_status = get_response(
            url=graph_ai_server + f'/image/extract_text/status/{task_id}',
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections
        )
        if response_text_status is None:
            return None
        response_text_status_json = response_text_status.json()
        text_status = response_text_status_json['task_status']
        if text_status in ['PENDING', 'STARTED']:
            sleep(1)
        elif text_status == 'SUCCESS':
            task_result = response_text_status_json['task_result']
            if not task_result['fresh']:
                StatusMSG(
                    f'text from {slide_token} has already been extracted in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'extraction of the text from {slide_token} failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            for result in task_result['result']:
                # we use document text detection which should perform better with coherent documents
                if result['method'] == 'ocr_google_1_token':
                    return {'text': result['text'], 'language': task_result['language']}
            StatusMSG(
                f'document text detection result not found',
                Color='yellow', Sections=list(sections) + ['WARNING']
            )
            return {'text': task_result['result'][0]['text'], 'language': task_result['language']}
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of text extraction for {slide_token}: '
                + text_status
            )


def extract_text(slide_tokens, force=False, slides_language=None):
    n_slide = len(slide_tokens)
    if n_slide == 0:
        return []
    # extract text (using google OCR) from the slides extracted with extract_slides()
    slides_text = []
    slides_timestamp = []
    for slide_index_str in sorted(slide_tokens.keys(), key=int):
        slide_token_dict = slide_tokens[slide_index_str]
        slide_token = slide_token_dict['token']
        slide_timestamp = slide_token_dict['timestamp']
        slide_text = extract_text_from_slide(
            slide_token, force=force, sections=('GRAPHAI', 'OCR', f'SLIDE {slide_index_str}/{n_slide}')
        )
        slides_text.append(slide_text)
        slides_timestamp.append(slide_timestamp)
    if slides_language is None:
        # get slides language
        slides_language_count = {}
        for slide_text in slides_text:
            if slide_text is None:
                continue
            language = slide_text['language']
            slides_language_count[language] = slides_language_count.get(language, 0) + 1
        slides_language_count_filtered = {
            lang: text for lang, text in slides_language_count.items() if lang in ['fr', 'en', 'de', 'it']}
        if len(slides_language_count_filtered) > 0:
            slides_language = max(slides_language_count_filtered, key=slides_language_count.get)
        else:
            slides_language = max(slides_language_count, key=slides_language_count.get)
    result_slides_text = []
    for slide_idx in range(len(slides_text)):
        if slides_text[slide_idx] is None:
            text_in_slide = None
        else:
            text_in_slide = slides_text[slide_idx]['text']
        result_slides_text.append({slides_language: text_in_slide, 'timestamp': slides_timestamp[slide_idx]})
    return result_slides_text


def translate_text(text: str, source_language, target_language, sections=('GRAPHAI', 'TRANSLATE'), force=False):
    if text is None or text == "" or source_language == target_language:
        return text
    # if len(text.strip()) < 3:
    #     return text
    response_translate = get_response(
        url=graph_ai_server + '/translation/translate',
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json={"text": text, "source": source_language, "target": target_language, "force": force},
        sections=sections
    )
    if response_translate is None:
        return None
    task_id = response_translate.json()['task_id']
    # wait for the translation to be completed
    tries_translate_status = 0
    while tries_translate_status < 6000:
        tries_translate_status += 1
        response_translate_status = get_response(
            url=graph_ai_server + f'/translation/translate/status/{task_id}',
            request_func=get,
            headers={'Content-Type': 'application/json'},
            sections=sections
        )
        if response_translate_status is None:
            return None
        response_translate_status_json = response_translate_status.json()
        translate_status = response_translate_status_json['task_status']
        if translate_status in ['PENDING', 'STARTED'] or response_translate_status_json['task_result'] is None:
            sleep(1)
        elif translate_status == 'SUCCESS':
            task_result = response_translate_status_json['task_result']
            if not task_result['fresh']:
                StatusMSG(
                    f'text has already been translated in the past',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if not task_result['successful']:
                StatusMSG(
                    f'translation of the text failed',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            if task_result['text_too_large']:
                StatusMSG(
                    f'text was too large to be translated',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            return task_result['result']
        elif translate_status == 'FAILURE':
            return None
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of translation for text: '
                + translate_status
            )


def translate_extracted_text(
        slides_text, source_language=None, destination_languages=('fr', 'en'), force=False
):
    n_slide = len(slides_text)
    for idx, slide_text in enumerate(slides_text):
        sections = ('GRAPHAI', 'TRANSLATE', f'SLIDE {idx+1}/{n_slide}')
        if source_language is None:
            source_language = None
            for k in slide_text.keys():
                if k != 'timestamp':
                    source_language = k
                    break
            if source_language is None:
                raise ValueError(f'could not determine the language of the slide {idx+1}: {slide_text}')
        for lang in destination_languages:
            if source_language != lang:
                translated_text = translate_text(
                    slide_text[source_language], source_language, lang,
                    sections=sections, force=force
                )
                if translated_text is None:
                    StatusMSG(
                        f'failed to translate "{slide_text[source_language]}"',
                        Color='yellow', Sections=list(sections) + ['WARNING']
                    )
                    slide_text[lang] = slide_text[source_language]
                else:
                    slide_text[lang] = translated_text
    return slides_text


def transcribe_audio(audio_token, force=False, force_lang=None, sections=('GRAPHAI', 'TRANSCRIBE')):
    json_data = {"token": audio_token, "force": force}
    if force_lang is not None:
        json_data["force_lang"] = force_lang
    response_transcribe = get_response(
        url=graph_ai_server + '/voice/transcribe',
        request_func=post,
        headers={'Content-Type': 'application/json'},
        json=json_data,
        sections=sections
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
            sections=sections
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
                for segment in task_result['subtitle_result']
            ]
            return task_result['language'], segments
        else:
            raise ValueError(
                f'Unexpected status while requesting the status of transcription for {audio_token}: '
                + transcribe_status
            )


def translate_subtitles(segments, source_language=None, destination_languages=('fr', 'en'), force=False):
    if source_language is None:
        for k in segments.keys():
            if k in ['start', 'end']:
                continue
            source_language = k
            break
        if source_language is None:
            raise ValueError(f'could not determine the language of the transcription: {segments}')
    for lang in destination_languages:
        if source_language != lang:
            n_segment = len(segments)
            for idx, segment in enumerate(segments):
                sections = ('GRAPHAI', 'TRANSLATE', f'SUBTITLE {idx+1}/{n_segment}')
                translated_text = translate_text(
                    segment[source_language], source_language, lang,
                    sections=sections, force=force
                )
                if translated_text is None:
                    StatusMSG(
                        f'failed to translate "{segment[source_language]}"',
                        Color='yellow', Sections=list(sections)
                    )
                    segments[idx][lang] = segment[source_language]
                else:
                    segments[idx][lang] = translated_text.strip()
    return segments


def translate_transcription(
        transcription, source_language=None, destination_languages=('fr', 'en'), force=False,
        sections=('GRAPHAI', 'TRANSLATE', 'TRANSCRIPTION')
):
    if source_language is None:
        for k in transcription.keys():
            source_language = k
            break
        if source_language is None:
            raise ValueError(f'could not determine the language of the transcription: {transcription}')
    for lang in destination_languages:
        if source_language != lang:
            translated_text = translate_text(
                transcription[source_language], source_language, lang,
                sections=sections, force=force
            )
            if translated_text is None:
                StatusMSG(
                    f'failed to translate "{transcription[source_language]}"',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            transcription[lang] = translated_text
    return transcription


def process_slides(video_token, force=False, slides_language=None):
    StatusMSG(
        f'extracting slides',
        Color='grey', Sections=['GRAPHAI', 'EXTRACT SLIDES', 'PROCESSING']
    )
    slide_tokens = extract_slides(video_token, force=force)
    if slide_tokens is None:
        slides = None
    else:
        StatusMSG(
            f'extracting text from {len(slide_tokens)} slides',
            Color='grey', Sections=['GRAPHAI', 'EXTRACT TEXT FROM SLIDES', 'PROCESSING']
        )
        slides_text = extract_text(slide_tokens, force=force, slides_language=slides_language)
        if slides_language is None and len(slides_text) > 0:
            slides_language = [k for k in slides_text[0].keys() if k != 'timestamp'][0]
        StatusMSG(
            f'translate text from {len(slides_text)} slides',
            Color='grey', Sections=['GRAPHAI', 'TRANSLATE', 'PROCESSING']
        )
        slides_text = translate_extracted_text(slides_text, force=force, source_language=slides_language)
        slides = []
        for slide_idx_str in sorted(slide_tokens.keys(), key=int):
            slide = {
                'token': slide_tokens[slide_idx_str]['token'],
                'timestamp': slide_tokens[slide_idx_str]['timestamp']
            }
            for k, v in slides_text[int(slide_idx_str) - 1].items():
                if k != 'timestamp':
                    slide[k] = v
            slides.append(slide)
    return slides_language, slides


def process_audio(video_token, force=False, audio_language=None):
    StatusMSG(
        f'extracting audio',
        Color='grey', Sections=['GRAPHAI', 'EXTRACT AUDIO', 'PROCESSING']
    )
    audio_token = extract_audio(video_token, force=force)
    if audio_token is None:
        segments = None
    else:
        StatusMSG(
            f'transcribe audio',
            Color='grey', Sections=['GRAPHAI', 'TRANSCRIBE', 'PROCESSING']
        )
        audio_language, segments = transcribe_audio(audio_token, force=force, force_lang=audio_language)
        StatusMSG(
            f'translate transcription for {len(segments)} segments',
            Color='grey', Sections=['GRAPHAI', 'TRANSLATE', 'PROCESSING']
        )
        if segments is not None:
            segments = translate_subtitles(segments, force=force, source_language=audio_language)
    return audio_language, segments


def process_video(
        video_url, force=False, audio_language=None, slides_language=None, analyze_audio=True, analyze_slides=True
):
    StatusMSG(
        f'processing the video {video_url}',
        Color='grey', Sections=['GRAPHAI', 'DOWNLOAD VIDEO', 'PROCESSING']
    )
    video_token = get_video_token(video_url)
    if video_token is None:
        return None
    # video_fingerprint = fingerprint_video(video_token)
    if analyze_slides:
        slides_language, slides = process_slides(video_token, force=force, slides_language=slides_language)
    else:
        slides_language = None
        slides = None
    if analyze_audio:
        audio_language, segments = analyze_audio(video_token, force=force, audio_language=audio_language)
    else:
        audio_language = None
        segments = None
    StatusMSG(
        f'The video {video_url} has been successfully processed',
        Color='green', Sections=['GRAPHAI', 'VIDEO', 'SUCCESS']
    )
    return dict(
        url=video_url,
        video_token=video_token,
        slides=slides,
        slides_language=slides_language,
        subtitles=segments,
        audio_language=audio_language,
    )


if __name__ == '__main__':
    import json

    # --------------------
    # new kaltura videos
    # --------------------
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_003ipc0i/format/download/protocol/https/flavorParamIds/0'
    url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_003zuhve/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_005blefe/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_005gbz9k/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_009hu1fy/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_009io2ie/format/download/protocol/https/flavorParamIds/0' # audio_language='fr'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00bqm9i3/format/download/protocol/https/flavorParamIds/0' # 20min processing / 30min video
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00fajklv/format/download/protocol/https/flavorParamIds/0' # 21min processing (3min slide extr/ 5+4min translate/ 8min transcribe)/41min video
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00gdquzv/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00h8gj93/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00ie24lu/format/download/protocol/https/flavorParamIds/0' # (10min slide extr/ 3min OCR/ 27min translate ocr/ 31min transcribe/ 18+min translate audio) 100 min video

    # --------------------
    # switchtube videos already processed
    # --------------------
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_vawt8zma/format/download/protocol/https/flavorParamIds/0' # switchId=wSIJK0iYvt
    video_info = process_video(url, force=True)
    print(video_info)

