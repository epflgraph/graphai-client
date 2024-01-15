from graphai.utils import status_msg, add_initial_disclaimer
from graphai.client_api.image import extract_text_from_slide
from graphai.client_api.translation import translate_text
from graphai.client_api.video import extract_slides, extract_audio, get_video_token
from graphai.client_api.voice import transcribe_audio, detect_language


def process_video(
        video_url, force=False, audio_language=None, slides_language=None,
        detect_audio_language=False, analyze_audio=True, analyze_slides=True,
        destination_languages=('fr', 'en'), graph_ai_server='http://127.0.0.1:28800', debug=False
):
    """
    Process the video whose URL is given as argument.
    :param video_url: URL of the video to process
    :param force: if True, the cache is ignored and all operations are performed.
    :param audio_language: if not None, language detection is skipped and the transcription is performed in the
        specified language.
    :param slides_language: if not None, language detection is skipped and the OCR is performed in the specified
        language.
    :param detect_audio_language: should the audio be extracted, and language detection on the audio be done.
        Only useful if analyze_audio is False and audio_language is None.
    :param analyze_audio: should the audio be extracted, transcription done and then translation if needed.
    :param analyze_slides: should slides be extracted, then OCR performed and then translation if needed.
    :param destination_languages: tuple of target languages. Perform translations if needed.
    :param graph_ai_server: address of the graphAI server (including protocol and port, f.e. "http://127.0.0.1:28800").
    :param debug: if True debug output is enabled.
    :return: a dictionary containing the results ot the processing.
    """
    status_msg(
        f'processing the video {video_url}',
        color='grey', sections=['GRAPHAI', 'DOWNLOAD VIDEO', 'PROCESSING']
    )
    video_token = get_video_token(video_url, graph_ai_server=graph_ai_server, debug=debug, force=force)
    if video_token is None:
        return None
    if analyze_slides:
        slides_language, slides = process_slides(
            video_token, force=force, slides_language=slides_language, destination_languages=destination_languages,
            graph_ai_server=graph_ai_server, debug=debug
        )
    else:
        slides_language = None
        slides = None
    if analyze_audio:
        audio_language, segments = process_audio(
            video_token, force=force, audio_language=audio_language, destination_languages=destination_languages,
            graph_ai_server=graph_ai_server, debug=debug
        )
    elif detect_audio_language:
        audio_language, segments = process_audio(
            video_token, force=force, audio_language=audio_language, only_detect_language=True,
            graph_ai_server=graph_ai_server, debug=debug
        )
    else:
        audio_language = None
        segments = None
    status_msg(
        f'The video {video_url} has been successfully processed',
        color='green', sections=['GRAPHAI', 'VIDEO', 'SUCCESS']
    )
    return dict(
        url=video_url,
        video_token=video_token,
        slides=slides,
        slides_language=slides_language,
        subtitles=segments,
        audio_language=audio_language,
    )


def process_slides(
        video_token, force=False, slides_language=None, destination_languages=('fr', 'en'),
        graph_ai_server='http://127.0.0.1:28800', debug=False
):
    """
    Extract slides from a video, perform OCR and translate the text.
    :param video_token: token associated with a video, typically the result of a call to get_video_token()
    :param force: if True, the cache is ignored and all operations are performed.
    :param slides_language: if not None, language detection is skipped and the OCR is performed in the specified
        language.
    :param destination_languages: tuple of target languages. Perform translations if needed.
    :param graph_ai_server: address of the graphAI server (including protocol and port, f.e. "http://127.0.0.1:28800").
    :param debug: if True debug output is enabled.
    :return: a 2-tuple containing first the language detected by the OCR (or forced by slides_language) and
        second a dictionary containing the result of the processing.
    """
    status_msg(
        f'extracting slides',
        color='grey', sections=['GRAPHAI', 'EXTRACT SLIDES', 'PROCESSING']
    )
    slide_tokens = extract_slides(video_token, force=force, graph_ai_server=graph_ai_server, debug=debug)
    if slide_tokens is None:
        slides = None
    else:
        status_msg(
            f'extracting text from {len(slide_tokens)} slides',
            color='grey', sections=['GRAPHAI', 'EXTRACT TEXT FROM SLIDES', 'PROCESSING']
        )
        slides_text = extract_text_from_slides(
            slide_tokens, force=force, slides_language=slides_language, graph_ai_server=graph_ai_server, debug=debug
        )
        if slides_language is None and len(slides_text) > 0:
            # single language statistically determined in extract_text_from_slides(), so we can just get the 1st result
            slides_language = [k for k in slides_text[0].keys() if k != 'timestamp'][0]
        if slides_language not in ['en', 'fr', 'de', 'it']:
            status_msg(
                f'OCR was detected as {slides_language} which is not supported, OCR discarded',
                color='yellow', sections=['GRAPHAI', 'EXTRACT TEXT FROM SLIDES', 'WARNING']
            )
            slides_language = None
        if slides_language is None:
            # we try to force english if OCR failed
            status_msg(
                f'try to force English while doing OCR',
                color='yellow', sections=['GRAPHAI', 'EXTRACT TEXT FROM SLIDES', 'WARNING']
            )
            slides_text = extract_text_from_slides(
                slide_tokens, force=force, slides_language='en', graph_ai_server=graph_ai_server, debug=debug
            )
            slides_language = 'en'
        status_msg(
            f'translate text from {len(slides_text)} slides in {slides_language}',
            color='grey', sections=['GRAPHAI', 'TRANSLATE', 'PROCESSING']
        )
        slides_text = translate_extracted_text(
            slides_text, force=force, source_language=slides_language, destination_languages=destination_languages,
            graph_ai_server=graph_ai_server, debug=debug
        )
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


def process_audio(
        video_token, force=False, only_detect_language=False, audio_language=None, destination_languages=('fr', 'en'),
        graph_ai_server='http://127.0.0.1:28800', debug=False
):
    """
    Extract audio from a video, perform transcription and translate the text.
    :param video_token: token associated with a video, typically the result of a call to get_video_token()
    :param force: if True, the cache is ignored and all operations are performed.
    :param only_detect_language: perform only language detection on the audio (skip transcription and translation).
    :param audio_language: if not None, language detection is skipped and the transcription is performed in the
        specified language.
    :param destination_languages: tuple of target languages. Perform translations if needed.
    :param graph_ai_server: address of the graphAI server (including protocol and port, f.e. "http://127.0.0.1:28800").
    :param debug: if True debug output is enabled.
    :return: a 2-tuple containing first the language detected for the audio (or forced by audio_language) and
        second a dictionary containing the result of the processing (None if only_detect_language is True).
    """
    status_msg(
        f'extracting audio',
        color='grey', sections=['GRAPHAI', 'EXTRACT AUDIO', 'PROCESSING']
    )
    audio_token = extract_audio(video_token, force=force, graph_ai_server=graph_ai_server, debug=debug)
    if audio_token is None:
        segments = None
    else:
        if only_detect_language:
            if not audio_language:
                audio_language = detect_language(audio_token, force=force, graph_ai_server=graph_ai_server, debug=debug)
            return audio_language, None
        status_msg(
            f'transcribe audio',
            color='grey', sections=['GRAPHAI', 'TRANSCRIBE', 'PROCESSING']
        )
        audio_language, segments = transcribe_audio(
            audio_token, force=force, force_lang=audio_language, graph_ai_server=graph_ai_server, debug=debug,
            strict=True
        )
        if audio_language not in ['en', 'fr', 'de', 'it']:
            status_msg(
                f'Audio language was detected as {audio_language} which is not supported, transcription discarded.',
                color='yellow', sections=['GRAPHAI', 'TRANSCRIBE', 'WARNING']
            )
            audio_language = None
        if audio_language is None:
            # we try to force english if transcription failed
            status_msg(
                f'try to force English while transcribing audio',
                color='yellow', sections=['GRAPHAI', 'TRANSCRIBE', 'WARNING']
            )
            audio_language, segments = transcribe_audio(
                audio_token, force=force, force_lang='en', graph_ai_server=graph_ai_server, debug=debug, strict=True
            )
            if not segments:
                audio_language = None
        if audio_language is None:
            # we try to force french if transcription failed
            status_msg(
                f'try to force French while transcribing audio',
                color='yellow', sections=['GRAPHAI', 'TRANSCRIBE', 'WARNING']
            )
            audio_language, segments = transcribe_audio(
                audio_token, force=force, force_lang='fr', graph_ai_server=graph_ai_server, debug=debug, strict=True
            )
            if not segments:
                audio_language = None
        if segments:
            status_msg(
                f'translate transcription for {len(segments)} segments in {audio_language}',
                color='grey', sections=['GRAPHAI', 'TRANSLATE', 'PROCESSING']
            )
            segments = translate_subtitles(
                segments, force=force, source_language=audio_language, destination_languages=destination_languages,
                graph_ai_server=graph_ai_server, debug=debug
            )
            segments = add_initial_disclaimer(segments)
        else:
            audio_language = None
    return audio_language, segments


def extract_text_from_slides(
        slide_tokens, force=False, slides_language=None, graph_ai_server='http://127.0.0.1:28800', debug=False
):
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
            slide_token, force=force, graph_ai_server=graph_ai_server,
            sections=('GRAPHAI', 'OCR', f'SLIDE {slide_index_str}/{n_slide}'), debug=debug
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
        elif len(slides_language_count) > 0:
            slides_language = max(slides_language_count, key=slides_language_count.get)
        else:
            return []
    result_slides_text = []
    for slide_idx in range(len(slides_text)):
        if slides_text[slide_idx] is None:
            text_in_slide = None
        else:
            text_in_slide = slides_text[slide_idx]['text']
        result_slides_text.append({slides_language: text_in_slide, 'timestamp': slides_timestamp[slide_idx]})
    return result_slides_text


def translate_extracted_text(
        slides_text, source_language=None, destination_languages=('fr', 'en'), force=False,
        graph_ai_server='http://127.0.0.1:28800', debug=False
):
    n_slide = len(slides_text)
    sections = ('GRAPHAI', 'TRANSLATE', f'{n_slide} SLIDES')
    if source_language is None:
        for idx, slide_text in enumerate(slides_text):
            language_slides = {}
            for k in slide_text.keys():
                if k != 'timestamp':
                    language_slides[k] = language_slides.get(k, 0) + 1
            try:
                # get the language detected for the most slides
                source_language = max(language_slides, key=lambda x: language_slides[x])
            except TypeError:
                raise ValueError(
                    f'could not determine the language used in most of the slides. The count is: {language_slides}'
                )
    text_to_translate = [slide_text[source_language] for slide_text in slides_text]
    for lang in destination_languages:
        if source_language != lang:
            translated_text = translate_text(
                text_to_translate, source_language, lang, graph_ai_server=graph_ai_server,
                sections=sections, force=force, debug=debug
            )
            if translated_text is None:
                status_msg(
                    f'failed to translate "{text_to_translate}" from {source_language} into {lang}',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
            # take care of a quirk of the API: when translating a list of length 1, the result is a string
            elif len(text_to_translate) != 1 and len(translated_text) != len(text_to_translate):
                status_msg(
                    f'Error during the translation of "{text_to_translate}", '
                    f'the translation has a different length: {translated_text}',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
            elif len(text_to_translate) == 1 and isinstance(translated_text, str):
                slides_text[0][lang] = translated_text
            else:
                for idx, slide_translated_text in enumerate(translated_text):
                    slides_text[idx][lang] = slide_translated_text
    return slides_text


def translate_subtitles(
        segments, source_language=None, destination_languages=('fr', 'en'), force=False,
        graph_ai_server='http://127.0.0.1:28800', debug=False
):
    n_segment = len(segments)
    sections = ('GRAPHAI', 'TRANSLATE', f'{n_segment} SUBTITLES')
    if source_language is None:
        language_segments = {}
        for segment in segments:
            for k in segment.keys():
                if k in ['start', 'end']:
                    continue
                language_segments[k] = language_segments.get(k, 0) + 1
        try:
            # get the language detected for the most slides
            source_language = max(language_segments, key=lambda x: language_segments[x])
        except TypeError:
            raise ValueError(
                f'could not determine the language used in most of the segments. The count is: {language_segments}'
            )
    text_to_translate = [seg[source_language].replace('\n', ' ') for seg in segments]
    for lang in destination_languages:
        if source_language != lang:
            translated_text = translate_text(
                text_to_translate, source_language, lang, graph_ai_server=graph_ai_server,
                sections=sections, force=force, debug=debug
            )
            if translated_text is None:
                status_msg(
                    f'failed to translate "{text_to_translate}"',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
            elif len(text_to_translate) != 1 and len(translated_text) != len(text_to_translate):
                status_msg(
                    f'Error during the translation of "{text_to_translate}", '
                    f'the translation has a different length: {translated_text}',
                    color='yellow', sections=list(sections) + ['WARNING']
                )
            elif len(text_to_translate) == 1 and isinstance(translated_text, str):
                segments[0][lang] = translated_text
            else:
                for idx, translated_segment in enumerate(translated_text):
                    if translated_segment is None:
                        segments[idx][lang] = None
                    else:
                        segments[idx][lang] = translated_segment.strip()
    return segments


if __name__ == '__main__':
    import json

    # --------------------
    # few kaltura videos
    # --------------------
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_003ipc0i/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_003zuhve/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_005blefe/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_005gbz9k/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_009hu1fy/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_009io2ie/format/download/protocol/https/flavorParamIds/0' # audio_language='fr'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00bqm9i3/format/download/protocol/https/flavorParamIds/0' # 20min processing / 30min video
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00h8gj93/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00ie24lu/format/download/protocol/https/flavorParamIds/0' # (10min slide extr/ 3min OCR/ 27min translate ocr/ 31min transcribe/ 18+min translate audio) 100 min video

    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00gdquzv/format/download/protocol/https/flavorParamIds/0'  # 40s video FAST!
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00fajklv/format/download/protocol/https/flavorParamIds/0' # 48min video
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_0gn6xnrf/format/download/protocol/https/flavorParamIds/0'  # german

    # video with no voice:
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_191runee/format/download/protocol/https/flavorParamIds/0'  # music
    # url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_i8zqj20g/format/download/protocol/https/flavorParamIds/0'  # silence
    # url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_xuvg6ca5/format/download/protocol/https/flavorParamIds/0'  # off voice + music
    # url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_u1offnl2/format/download/protocol/https/flavorParamIds/0'  # music

    #url = "https://api.cast.switch.ch/p/113/sp/11300/serveFlavor/entryId/0_vgsq7l1z/v/2/flavorId/0_lm3u67lv/fileName/Result_of_trial15_C001H001S0001_(Source).avi/forceproxy/true/name/a.avi"  # no slide/no sound
    #video_info = process_video(url, force=True, graph_ai_server='http://127.0.0.1:28800', analyze_slides=False, debug=True)
    #print(video_info)
    from graphai.rcp import process_videos_on_rcp

    process_videos_on_rcp(['0_sltslw7z'], force=True, debug=True)

