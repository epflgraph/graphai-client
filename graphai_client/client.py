from multiprocessing import Pool
from typing import Tuple, Dict, Optional, Any
from graphai_client.utils import (
    status_msg, add_initial_disclaimer, get_video_link_and_size, get_video_id_and_platform, execute_query
)
from graphai_client.client_api.utils import login
from graphai_client.client_api.image import (
    extract_text_from_slide, calculate_fingerprint as calculate_slide_fingerprint
)
from graphai_client.client_api.video import extract_slides, extract_audio, get_video_token
from graphai_client.client_api.voice import (
    transcribe_audio, detect_language, calculate_fingerprint as calculate_audio_fingerprint
)
from graphai_client.client_api.translation import translate_text


def process_video(
        video_url, force=False, force_download=False, audio_language=None, slides_language=None,
        detect_audio_language=False, analyze_audio=True, analyze_slides=True, destination_languages=('fr', 'en'),
        graph_api_json=None, login_info=None, debug=False, google_api_token: str = None
):
    """
    Process the video whose URL is given as argument.

    :param video_url: URL of the video to process
    :param force: if True, the cache is ignored and all operations are performed.
    :param force_download: if True, the downloading of the video is forced even if it is present in the cache.
    :param audio_language: if not None, language detection is skipped and the transcription is performed in the
        specified language.
    :param slides_language: if not None, language detection is skipped and the OCR is performed in the specified
        language.
    :param detect_audio_language: should the audio be extracted, and language detection on the audio be done.
        Only useful if analyze_audio is False and audio_language is None.
    :param analyze_audio: should the audio be extracted, transcription done and then translation if needed.
    :param analyze_slides: should slides be extracted, then OCR performed and then translation if needed.
    :param destination_languages: tuple of target languages. Perform translations if needed.
    :param graph_api_json: path to a json file with login info: host (incl. protocol and port), user and password).
        Defaults to graphai/config/graphai-api.json
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json)
        If not set, a login will be performed to populate it using the info from graph_api_json.
    :param debug: if True additional information about each connection to the API is displayed.
    :param google_api_token: the google API token charged for the use of the OCR.
    :return: a dictionary containing the results ot the processing.
    """
    if login_info is None or 'token'not in login_info:
        login_info = login(graph_api_json)
    video_token, video_size, streams = download_url(
        video_url, login_info, force=force, force_download=force_download, debug=debug
    )
    if video_token is None:
        return None
    codec_types = [s['codec_type'] for s in streams]
    slides = None
    if analyze_slides:
        if 'video' in codec_types:
            slides_language, slides = process_slides(
                video_token, login_info, force=force, slides_language=slides_language,
                destination_languages=destination_languages, debug=debug, google_api_token=google_api_token
            )
        else:
            slides_language = 'NA'
    segments = None
    audio_fingerprint = None
    if analyze_audio:
        if 'audio' in codec_types:
            audio_language, audio_fingerprint, segments = process_audio(
                video_token, login_info, force=force, audio_language=audio_language,
                destination_languages=destination_languages, debug=debug
            )
        else:
            audio_language = 'NA'
    elif detect_audio_language:
        if 'audio' in codec_types:
            audio_language, audio_fingerprint, _ = process_audio(
                video_token, login_info, force=force, audio_language=audio_language, only_detect_language=True,
                debug=debug
            )
        else:
            audio_language = 'NA'
    return dict(
        url=video_url,
        video_size=video_size,
        video_token=video_token,
        slides=slides,
        slides_language=slides_language,
        subtitles=segments,
        audio_language=audio_language,
        audio_fingerprint=audio_fingerprint,
        streams=streams
    )


def process_slides(
        video_token, login_info, force=False, slides_language=None, destination_languages=('fr', 'en'), debug=False,
        google_api_token: str = None
):
    """
    Extract slides from a video, perform OCR and translate the text.
    :param video_token: token associated with a video, typically the result of a call to get_video_token()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json)
    :param force: if True, the cache is ignored and all operations are performed.
    :param slides_language: if not None, language detection is skipped and the OCR is performed in the specified
        language.
    :param destination_languages: tuple of target languages. Perform translations if needed.
        Translation is skipped if set to None
    :param debug: if True debug output is enabled.
    :param google_api_token: the Google API token charged for the use of the OCR.
    :return: a 2-tuple containing first the language detected by the OCR (or forced by slides_language) and
        second a dictionary containing the result of the processing.
    """
    slide_tokens = extract_slides(video_token, login_info, force=force, debug=debug)
    if slide_tokens is None:
        slides = None
    else:
        slide_tokens = get_fingerprint_of_slides(slide_tokens, login_info, force=force, debug=debug)
        status_msg(
            f'extracting text from {len(slide_tokens)} slides',
            color='grey', sections=['GRAPHAI', 'EXTRACT TEXT FROM SLIDES', 'PROCESSING']
        )
        slides_text = extract_text_from_slides(
            slide_tokens, login_info, force=force, slides_language=slides_language, debug=debug,
            google_api_token=google_api_token
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
                slide_tokens, login_info, force=force, slides_language='en', debug=debug,
                google_api_token=google_api_token
            )
            slides_language = 'en'
        if destination_languages:
            status_msg(
                f'translate text from {len(slides_text)} {slides_language} slides to {", ".join(destination_languages)}',
                color='grey', sections=['GRAPHAI', 'TRANSLATE', 'PROCESSING']
            )
            slides_text = translate_extracted_text(
                slides_text, login_info, force=force, source_language=slides_language,
                destination_languages=destination_languages, debug=debug
            )
        slides = []
        for slide_idx_str in sorted(slide_tokens.keys(), key=int):
            slide = {
                'token': slide_tokens[slide_idx_str]['token'],
                'timestamp': slide_tokens[slide_idx_str]['timestamp'],
                'fingerprint': slide_tokens[slide_idx_str]['fingerprint']
            }
            for k, v in slides_text[int(slide_idx_str) - 1].items():
                if k != 'timestamp':
                    slide[k] = v
            slides.append(slide)
        status_msg(
            f'Successfully extracted text from {len(slide_tokens)} slides',
            color='green', sections=['GRAPHAI', 'EXTRACT TEXT FROM SLIDES', 'SUCCESS']
        )
    return slides_language, slides


def process_audio(
        video_token, login_info, force=False, only_detect_language=False, audio_language=None,
        destination_languages=('fr', 'en'), debug=False, sections=('GRAPHAI', 'TRANSCRIBE')
) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """
    Extract audio from a video, perform transcription and translate the text.
    :param video_token: token associated with a video, typically the result of a call to get_video_token()
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json)
    :param force: if True, the cache is ignored and all operations are performed.
    :param only_detect_language: perform only language detection on the audio (skip transcription and translation).
    :param audio_language: if not None, language detection is skipped and the transcription is performed in the
        specified language.
    :param destination_languages: tuple of target languages. Perform translations if needed.
         Translation is skipped if set to None
    :param debug: if True debug output is enabled.
    :return: a 3-tuple containing first the language detected for the audio (or forced by audio_language),
        the audio fingerprint and a dictionary containing the result of the processing
        (or None if only_detect_language is True).
    """
    audio_token = extract_audio(video_token, login_info, force=force, debug=debug)
    if audio_token is None:
        segments = None
        audio_fingerprint = None
    else:
        audio_fingerprint = calculate_audio_fingerprint(audio_token, login_info, force=force, debug=debug)
        if only_detect_language:
            if not audio_language:
                audio_language = detect_language(audio_token, login_info, force=force, debug=debug)
            return audio_language, audio_fingerprint, None
        audio_language, segments = transcribe_audio(
            audio_token, login_info, force=force, force_lang=audio_language, debug=debug, strict=True
        )
        if audio_language not in ['en', 'fr', 'de', 'it', None]:
            status_msg(
                f'Audio language was detected as {audio_language} which is not supported, transcription discarded.',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            audio_language = None
        if audio_language is None:
            # we try to force english if transcription failed
            status_msg(
                f'try to force English while transcribing audio',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            audio_language, segments = transcribe_audio(
                audio_token, login_info, force=force, force_lang='en', debug=debug, strict=True, sections=sections
            )
            if not segments:
                audio_language = None
        if audio_language is None:
            # we try to force french if transcription failed
            status_msg(
                f'try to force French while transcribing audio',
                color='yellow', sections=list(sections) + ['WARNING']
            )
            audio_language, segments = transcribe_audio(
                audio_token, login_info, force=force, force_lang='fr', debug=debug, strict=True, sections=sections
            )
            if not segments:
                audio_language = None
        if segments:
            if destination_languages:
                segments = translate_subtitles(
                    segments, login_info, force=force, source_language=audio_language,
                    destination_languages=destination_languages, debug=debug
                )
            segments = add_initial_disclaimer(segments)
        else:
            audio_language = None
    return audio_language, audio_fingerprint, segments


def download_url(video_url, login_info, force=False, force_download=False, debug=False):
    _, platform = get_video_id_and_platform(video_url)
    if platform in ('youtube', ):
        max_download_time = 900
    else:
        _, octet_size = get_video_link_and_size(video_url)
        if not octet_size:
            return None, None, None
        max_download_time = max(int(octet_size/1048576), 900)  # 15 min or 1MB/s download
    video_token, video_size, streams = get_video_token(
        video_url, login_info, debug=debug, force=force or force_download, max_processing_time_s=max_download_time
    )
    return video_token, video_size, streams


def get_audio_fingerprint_of_video(
        video_url, login_info, force=False, force_download=False, debug=False
):
    video_token, video_size, streams = download_url(
        video_url, login_info, force=force, force_download=force_download, debug=debug
    )
    codec_types = [s['codec_type'] for s in streams]
    if video_token is None:
        return None
    audio_fingerprint = None
    if 'audio' in codec_types:
        audio_token = extract_audio(video_token, login_info, force=force, debug=debug)
        if audio_token is not None:
            audio_fingerprint = calculate_audio_fingerprint(audio_token, login_info, force=force, debug=debug)
    return audio_fingerprint


def get_video_information_from_streams(streams):
    video_information = {}
    try:
        audio_stream_idx_largest_bitrate = max({
            idx: stream['bit_rate']
            for idx, stream in enumerate(streams) if stream['codec_type'] == 'audio'
        })
        audio_stream = streams[audio_stream_idx_largest_bitrate]
        video_information['audio_bit_rate'] = audio_stream['bit_rate']
        video_information['audio_codec_name'] = audio_stream['codec_name']
        video_information['audio_duration'] = audio_stream['duration']
        video_information['audio_sample_rate'] = audio_stream['sample_rate']
    except ValueError:
        video_information['audio_bit_rate'] = None
        video_information['audio_codec_name'] = None
        video_information['audio_duration'] = None
        video_information['audio_sample_rate'] = None
    try:
        video_stream_idx_largest_bitrate = max({
            idx: stream['bit_rate']
            for idx, stream in enumerate(streams) if stream['codec_type'] == 'video'
        })
        video_stream = streams[video_stream_idx_largest_bitrate]
        video_information['video_bit_rate'] = video_stream['bit_rate']
        video_information['video_codec_name'] = video_stream['codec_name']
        video_information['video_duration'] = video_stream['duration']
        video_information['video_resolution'] = video_stream['resolution']
    except ValueError:
        video_information['video_bit_rate'] = None
        video_information['video_codec_name'] = None
        video_information['video_duration'] = None
        video_information['video_resolution'] = None
    return video_information


def extract_text_from_slides(
        slide_tokens: dict, login_info: dict, force=False, slides_language=None, debug=False, quiet=False,
        google_api_token: str = None
):
    """
    Extract text (using google OCR) from the slides extracted with extract_slides().
    The main language of the slides is statistically determined.
    :param slide_tokens: typically the output from extract_slides().
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json)
    :param force: if True, the cache is ignored and all operations are performed.
    :param slides_language: if not None, the statistical determination of the main language is skipped and the given
        value is used instead.
    :param debug: if True debug output is enabled.
    :param quiet: disable success status messages.
    :param google_api_token: the google API token charged for the use of the OCR.
    :return: a list of dictionaries with the timestamp and text of the slides. The detected language is used as key
        for the extracted text in each dictionary.
    """
    n_slide = len(slide_tokens)
    if n_slide == 0:
        return []
    #
    slides_text = []
    slides_timestamp = []
    for slide_index_str in sorted(slide_tokens.keys(), key=int):
        slide_token_dict = slide_tokens[slide_index_str]
        slide_token = slide_token_dict['token']
        slide_timestamp = slide_token_dict['timestamp']
        slide_text = extract_text_from_slide(
            slide_token, login_info, force=force, sections=('GRAPHAI', 'OCR', f'SLIDE {slide_index_str}/{n_slide}'),
            debug=debug, quiet=quiet, google_api_token=google_api_token
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
        slides_text, login_info, source_language=None, destination_languages=('fr', 'en'), force=False, debug=False
):
    """
    translate text extracted from slides
    :param slides_text: typically the output from extract_text_from_slides().
    :param login_info: dictionary with login information, typically return by graphai.client_api.login(graph_api_json).
    :param source_language: source language of the text extracted from the slides.
    :param destination_languages: target languages to translate the test to.
    :param force: if True, the cache is ignored and all operations are performed.
    :param debug: if True debug output is enabled.
    :return:
    """
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
                text_to_translate, source_language, lang, login_info, sections=sections, force=force, debug=debug
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
        segments: list, login_info: dict, source_language=None, destination_languages=('fr', 'en'),
        force=False, debug=False
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
                text_to_translate, source_language, lang, login_info, sections=sections, force=force, debug=debug
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


def _calculate_slide_fingerprint(index_str, token, login_info, force, debug):
    fingerprint = calculate_slide_fingerprint(
        token, login_info, force=force, debug=debug, quiet=True,
        sections=('KALTURA', 'FINGERPRINT', 'SLIDE ' + index_str)
    )
    return index_str, fingerprint


def get_fingerprint_of_slides(slide_tokens: dict, login_info: dict, force=False, debug=False):
    args = []
    for slide_index_str in sorted(slide_tokens.keys(), key=int):
        slide_info = slide_tokens[slide_index_str]
        slide_token = slide_info.get("token", None)
        if not slide_token:
            status_msg(
                f'Invalid token for slide {slide_token}, slide is skipped.',
                color='yellow', sections=['KALTURA', 'FINGERPRINT', 'SLIDES', 'WARNING']
            )
            continue
        token_status = slide_info.get("token_status", None)
        if not token_status:
            status_msg(
                f'Invalid token status for slide {slide_token}',
                color='yellow', sections=['KALTURA', 'FINGERPRINT', 'SLIDES', 'WARNING']
            )
        elif not token_status.get("active", None) and not token_status.get("fingerprinted", None):
            status_msg(
                f'Non-active and not-fingerprinted token status for slide {slide_token}',
                color='yellow', sections=['KALTURA', 'FINGERPRINT', 'SLIDES', 'WARNING']
            )
        args.append((slide_index_str, slide_token, login_info, force, debug))
    with Pool(processes=10) as pool:
        results = pool.starmap(_calculate_slide_fingerprint, args)
        for index_slide_str, slide_fingerprint in results:
            slide_tokens[index_slide_str]['fingerprint'] = slide_fingerprint
    return slide_tokens


def get_video_token_and_codec_types(
        platform, video_id, piper_connection, login_info, force=False, force_download=False, debug=False, sections=()
):
    video_info = execute_query(
        piper_connection,
        f'SELECT videoUrl FROM gen_video.Videos WHERE platform="{platform}" AND videoId="{video_id}";'
    )
    if len(video_info) != 1 or len(video_info[0]) != 1:
        status_msg(
            f'The video {video_id} on {platform} could not be found in gen_video.Videos',
            color='red', sections=list(sections) + ['ERROR']
        )
        return None, None
    video_url = video_info[0][0]
    video_token, video_size, streams = download_url(
        video_url, login_info, force=force, force_download=force_download, debug=debug
    )
    if video_token is None:
        status_msg(
            f'Download of the video {video_id} on {platform}  at {video_url} failed.',
            color='red', sections=list(sections) + ['ERROR']
        )
        return None, None
    codec_types = [s['codec_type'] for s in streams]
    return video_token, codec_types
