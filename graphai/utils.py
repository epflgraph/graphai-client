from time import sleep
from termcolor import cprint
from requests import get
from datetime import datetime, timedelta
from string import Formatter
from numpy import isnan, isinf
from re import compile

default_disclaimer = {
    'en': 'These subtitles have been generated automatically',
    'fr': 'Ces sous-titres ont été générés automatiquement',
    'de': 'Diese Untertitel wurden automatisch generiert',
    'it': 'Questi sottotitoli sono stati generati automaticamente'
}


def status_msg(msg, color=None, sections=(), print_flag=True):
    """
    Print a nice status message.

    :param msg: message to print.
    :type msg: str
    :param color: color of the message. If None, the default color is used. Available colors are:

        - 'grey', 'black', 'red', 'green', 'orange', 'blue', 'magenta', 'cyan', 'light gray', 'dark gray', 'light red',
            'light green', 'yellow', 'light purple', 'light cyan' and 'white' in terminal mode.
        - 'grey', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan' and 'white' in non-terminal mode.

    :type color: str, None
    :param sections: list of strings representing the sections which will be displayed at the beginning of the message.
    :type sections: list
    :param print_flag: If False nothing is printed.
    :type print_flag: bool
    """
    if not print_flag:
        return
    global_string = '[%s] ' % f"{datetime.now():%Y-%m-%d %H:%M:%S}"
    for section in sections:
        global_string += '[%s] ' % section
    global_string += msg
    cprint(global_string, color)


def get_video_link_and_size(video_url, retry=5, wait_retry=15):
    attempt = 0
    while attempt < retry:
        try:
            response = get(video_url, stream=True)
            if response.status_code == 200:
                return response.url, response.headers['Content-length']
            else:
                status_msg(
                    f'{video_url} not reachable, error {response.status_code}: {response.reason}', color='yellow',
                    sections=['KALTURA', 'CHECK URL', 'WARNING']
                )
                return None, None
        except Exception as e:
            attempt += 1
            status_msg(
                f'got Exception while getting {video_url}, error: {e} try {attempt}/{retry}', color='yellow',
                sections=['KALTURA', 'CHECK URL', 'WARNING']
            )
            sleep(wait_retry)
    return None, None


def strfdelta(time_delta: timedelta, fmt='{H:02}:{M:02}:{S:02},{m:03}'):
    """Convert a datetime.timedelta object to a custom-formatted string,
    just like the stftime() method does for datetime.datetime objects.

    The fmt argument allows custom formatting to be specified.  Fields can
    include milliseconds (`m`), seconds (`S`), minutes (`M`), hours (`H`), days (`D`), and weeks (`W`).

    Each field is optional.

    Some examples:
        '{H}:{M:02}:{S:02},{m:03}'        --> '8:04:02,000' (default)
        '{D:02}d {H:02}h {M:02}m {S:02}s' --> '05d 08h 04m 02s'
        '{W}w {D}d {H}:{M:02}:{S:02}'     --> '4w 5d 8:04:02'
        '{D:2}d {H:2}:{M:02}:{S:02}'      --> ' 5d  8:04:02'
        '{H}h {S}s'                       --> '72h 800s'
    """

    # Convert timedelta to integer milliseconds.
    remainder = int(time_delta.total_seconds()*1000)

    f = Formatter()
    desired_fields = [field_tuple[1] for field_tuple in f.parse(fmt)]
    possible_fields = ('W', 'D', 'H', 'M', 'S', 'm')
    constants = {'W': 604800 * 1000, 'D': 86400 * 1000, 'H': 3600 * 1000, 'M': 60 * 1000, 'S': 1 * 1000, 'm': 1}
    values = {}
    for field in possible_fields:
        if field in desired_fields and field in constants:
            values[field], remainder = divmod(remainder, constants[field])
    return f.format(fmt, **values)


def prepare_values_mysql(values, encoding='utf8'):
    values_str = []
    for val in values:
        val_str = str(val).encode(encoding, errors='ignore').decode(encoding)
        nan_or_inf = False
        try:
            val_float = float(val)
            nan_or_inf = isnan(val_float) or isinf(val_float)
        except (TypeError, ValueError):
            pass
        if val is None or nan_or_inf:
            values_str.append("NULL")
        elif val_str.isdigit():
            values_str.append(val_str)
        else:
            values_str.append(f"'" + val_str.replace("\\", "\\\\").replace("'", "\\'").replace(";", "\\;") + "'")
    return values_str


def insert_line_into_table(cursor, schema, table_name, columns, values, encoding='utf8'):
    values_str = prepare_values_mysql(values, encoding=encoding)
    sql_query = f"""
                INSERT INTO `{schema}`.`{table_name}` ({', '.join(columns)})
                VALUES ({', '.join(values_str)});
            """
    try:
        cursor.execute(sql_query)
    except Exception as e:
        msg = f'Error while inserting data in `{schema}`.`{table_name}`:\n'
        msg += f'the query was:\n {sql_query}'
        msg += 'the exception received was: ' + str(e)
        raise RuntimeError(msg)


def convert_subtitle_into_segments(caption_data, file_ext='srt', text_key='text'):
    caption_lines = caption_data.encode('utf8').decode('utf-8-sig', errors='ignore').split('\n')
    time1_regexp = r'(:?(:?(?P<h1>[\d]{1,2}):)?(?P<m1>[\d]{1,2}):)?(?P<s1>[\d]{1,2})(:?[.,](?P<subs1>[\d]{0,6}))?'
    time2_regexp = r'(:?(:?(?P<h2>[\d]{1,2}):)?(?P<m2>[\d]{1,2}):)?(?P<s2>[\d]{1,2})(:?[.,](?P<subs2>[\d]{0,6}))?'
    time_regexp = compile(r'[\s]*' + time1_regexp + r'[\s]+-->[\s]+' + time2_regexp + r'[\s]*')
    if file_ext == 'srt':
        current_line_type = 'id'
    elif file_ext == 'vtt':
        current_line_type = 'time'
        while not time_regexp.match(caption_lines[0]):
            caption_lines = caption_lines[1:]
    else:
        raise ValueError(f'Unsupported file extension {file_ext}, must be "srt" or "vtt"')
    segments = []
    text = ''
    start = 0
    end = 0
    segment_id = 0
    for line in caption_lines:
        if current_line_type == 'id':
            if line.strip():
                segment_id = int(line) - 1
                current_line_type = 'time'
        elif current_line_type == 'time':
            match_time = time_regexp.match(line)
            if match_time:
                time_dict = match_time.groupdict(default='0')
                start = int(time_dict.get('h1', 0)) * 3600 + int(time_dict.get('m1', 0)) * 60 + \
                        int(time_dict['s1']) + float('0.' + time_dict.get('subs1', 0))
                end = int(time_dict.get('h2', 0)) * 3600 + int(time_dict.get('m2', 0)) * 60 + \
                        int(time_dict['s2']) + float('0.' + time_dict.get('subs2', 0))
            else:
                raise RuntimeError(f'Expected segment start and end time but got: {line}')
            current_line_type = 'text'
            text = ''
        elif current_line_type == 'text':
            if line.strip():
                if text:
                    text += '\n' + line
                else:
                    text = line
            else:
                segments.append({'id': segment_id, 'start': start, 'end': end, text_key: text})
                text = ''
                segment_id += 1
                if file_ext == 'srt':
                    current_line_type = 'id'
                elif file_ext == 'vtt':
                    current_line_type = 'time'
    if text:
        segments.append({'id': segment_id, 'start': start, 'end': end, text_key: text})
    return segments


def combine_language_segments(text_key='text', **kwargs):
    segments_combined = []
    languages = list(kwargs.keys())
    n_segments = len(kwargs[languages[0]])
    for lang in languages[1:]:
        if len(kwargs[lang]) != n_segments:
            raise ValueError(f'the number of segment is not the same for {languages[0]} and {lang}')
    for lang, segments_lang in kwargs.items():
        for seg_idx, segment in enumerate(segments_lang):
            if len(segments_combined) <= seg_idx:
                segments_combined.append(
                    {
                        'id': segment.get('id', seg_idx),
                        'start': segment['start'], 'end': segment['end'],
                        lang: segment[text_key]
                    }
                )
            else:
                segment_equiv = segments_combined[seg_idx]
                if segment['start'] != segment_equiv['start'] or segment['end'] != segment_equiv['end']:
                    raise ValueError(f'{seg_idx}th segment timing are not the same for {languages[0]} and {lang}')
                segments_combined[seg_idx][lang] = segment[text_key]
    return segments_combined


def add_initial_disclaimer(segments, disclaimer_per_language=None, restrict_lang=None):
    """
    Add a disclaimer at the beginning of subtitles for languages in restrict_lang
    If the first subtitle appears before 2s, its start is changed to 0, otherwise a new segment is added for 0 to 2s.

    :param segments: subtitles as a list of dictionaries with id, start, end and the 2 letter language as keys
    :param disclaimer_per_language: a dictionary giving the disclaimer for each language
    :param restrict_lang: a tuple with the list o language for which the disclaimer must be added. It is added to all
        languages if restrict_lang=None
    :return:
    """
    if disclaimer_per_language is None:
        disclaimer_per_language = default_disclaimer
    segments_lang_to_modify = []
    segments_lang_to_keep = []
    for lang in segments[0].keys():
        if lang in ('id', 'start', 'end'):
            continue
        if restrict_lang and lang not in restrict_lang:
            segments_lang_to_keep.append(lang)
            continue
        # skip languages where all text is None
        if segments[0][lang] is None:
            if len([1 for seg in segments if seg[lang] is not None]) == 0:
                segments_lang_to_keep.append(lang)
                continue
        segments_lang_to_modify.append(lang)
    if segments[0]['start'] == 0:
        add_first_segment = False
        set_first_segment_start_to_zero = False
    elif segments[0]['start'] <= 2:
        add_first_segment = False
        set_first_segment_start_to_zero = True
    else:
        add_first_segment = True
        set_first_segment_start_to_zero = False
    modified_segments = []
    if add_first_segment:
        first_segment = {'id': 0, 'start': 0, 'end': 2}
        for lang in segments_lang_to_modify:
            first_segment[lang] = disclaimer_per_language.get(lang, '')
        modified_segments.append(first_segment)
    for idx, seg in enumerate(segments):
        seg_id = seg.get('id', idx)
        start = seg['start']
        end = seg['end']
        if seg_id == 0 and set_first_segment_start_to_zero:
            start = 0
        if add_first_segment:
            seg_id += 1
        modified_seg = {'id': seg_id, 'start': start, 'end': end}
        for lang in segments_lang_to_modify:
            text = seg.get(lang, None)
            if seg_id == 0:  # cannot happen here with add_first_segment=True
                if text is None:
                    text = disclaimer_per_language.get(lang, '')
                elif text.split('\n')[0] != disclaimer_per_language.get(lang, ''):
                    text = disclaimer_per_language.get(lang, '') + '\n' + text
            modified_seg[lang] = text
        for lang in segments_lang_to_keep:
            modified_seg[lang] = seg.get(lang, None)
        modified_segments.append(modified_seg)
    return modified_segments
