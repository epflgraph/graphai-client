from graphai.utils import StatusMSG
from graphai.api.image import extract_text_from_slide
from graphai.api.translation import translate_text
from graphai.api.video import extract_slides, extract_audio, get_video_token
from graphai.api.voice import transcribe_audio


def process_video(
        video_url, force=False, audio_language=None, slides_language=None, analyze_audio=True, analyze_slides=True,
        destination_languages=('fr', 'en'), debug=False
):
    """
    Process the video whose URL is given as argument.
    :param video_url: URL of the video to process
    :param force: if True, the cache is ignored and all operations are performed.
    :param audio_language: if not None, language detection is skipped and the transcription is performed in the
        specified language.
    :param slides_language: if not None, language detection is skipped and the OCR is performed in the specified
        language.
    :param analyze_audio: should the audio be extracted, transcription done and then translation if needed.
    :param analyze_slides: should slides be extracted, then OCR performed and then translation if needed.
    :param destination_languages: tuple of target languages. Perform translations if needed.
    :param debug: if True debug output is enabled.
    :return: a dictionary containing the results ot the processing.
    """
    StatusMSG(
        f'processing the video {video_url}',
        Color='grey', Sections=['GRAPHAI', 'DOWNLOAD VIDEO', 'PROCESSING']
    )
    video_token = get_video_token(video_url, debug=debug)
    if video_token is None:
        return None
    if analyze_slides:
        slides_language, slides = process_slides(
            video_token, force=force, slides_language=slides_language, destination_languages=destination_languages,
            debug=debug
        )
    else:
        slides_language = None
        slides = None
    if analyze_audio:
        audio_language, segments = process_audio(
            video_token, force=force, audio_language=audio_language, destination_languages=destination_languages,
            debug=debug
        )
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


def process_slides(video_token, force=False, slides_language=None, destination_languages=('fr', 'en'), debug=False):
    StatusMSG(
        f'extracting slides',
        Color='grey', Sections=['GRAPHAI', 'EXTRACT SLIDES', 'PROCESSING']
    )
    slide_tokens = extract_slides(video_token, force=force, debug=debug)
    if slide_tokens is None:
        slides = None
    else:
        StatusMSG(
            f'extracting text from {len(slide_tokens)} slides',
            Color='grey', Sections=['GRAPHAI', 'EXTRACT TEXT FROM SLIDES', 'PROCESSING']
        )
        slides_text = extract_text(slide_tokens, force=force, slides_language=slides_language, debug=debug)
        if slides_language is None and len(slides_text) > 0:
            slides_language = [k for k in slides_text[0].keys() if k != 'timestamp'][0]
        StatusMSG(
            f'translate text from {len(slides_text)} slides',
            Color='grey', Sections=['GRAPHAI', 'TRANSLATE', 'PROCESSING']
        )
        slides_text = translate_extracted_text(
            slides_text, force=force, source_language=slides_language, destination_languages=destination_languages,
            debug=debug
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


def process_audio(video_token, force=False, audio_language=None, destination_languages=('fr', 'en'), debug=False):
    StatusMSG(
        f'extracting audio',
        Color='grey', Sections=['GRAPHAI', 'EXTRACT AUDIO', 'PROCESSING']
    )
    audio_token = extract_audio(video_token, force=force, debug=debug)
    if audio_token is None:
        segments = None
    else:
        StatusMSG(
            f'transcribe audio',
            Color='grey', Sections=['GRAPHAI', 'TRANSCRIBE', 'PROCESSING']
        )
        audio_language, segments = transcribe_audio(audio_token, force=force, force_lang=audio_language, debug=debug)
        StatusMSG(
            f'translate transcription for {len(segments)} segments',
            Color='grey', Sections=['GRAPHAI', 'TRANSLATE', 'PROCESSING']
        )
        if segments is not None:
            segments = translate_subtitles(
                segments, force=force, source_language=audio_language, destination_languages=destination_languages,
                debug=debug
            )
    return audio_language, segments


def extract_text(
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


def translate_extracted_text(
        slides_text, source_language=None, destination_languages=('fr', 'en'), force=False,
        graph_ai_server='http://127.0.0.1:28800', debug=False
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
                    slide_text[source_language], source_language, lang, graph_ai_server=graph_ai_server,
                    sections=sections, force=force, debug=debug
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


def translate_subtitles(
        segments, source_language=None, destination_languages=('fr', 'en'), force=False,
        graph_ai_server='http://127.0.0.1:28800', debug=False
):
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
                    segment[source_language], source_language, lang, graph_ai_server=graph_ai_server,
                    sections=sections, force=force, debug=debug
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
        sections=('GRAPHAI', 'TRANSLATE', 'TRANSCRIPTION'), debug=False
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
                sections=sections, force=force, debug=debug
            )
            if translated_text is None:
                StatusMSG(
                    f'failed to translate "{transcription[source_language]}"',
                    Color='yellow', Sections=list(sections) + ['WARNING']
                )
            transcription[lang] = translated_text
    return transcription


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
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00fajklv/format/download/protocol/https/flavorParamIds/0' # 21min processing (3min slide extr/ 5+4min translate/ 8min transcribe)/41min video
    url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00gdquzv/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00h8gj93/format/download/protocol/https/flavorParamIds/0'
    #url = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/0_00ie24lu/format/download/protocol/https/flavorParamIds/0' # (10min slide extr/ 3min OCR/ 27min translate ocr/ 31min transcribe/ 18+min translate audio) 100 min video

    video_info = process_video(url, force=True)
    print(video_info)

