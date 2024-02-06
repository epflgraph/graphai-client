from time import sleep
from termcolor import cprint
from requests import get
from datetime import datetime, timedelta
from string import Formatter
from numpy import isnan, isinf
from re import compile, finditer
from typing import Literal
from mysql.connector.cursor import MySQLCursor
from mysql.connector.cursor_cext import CMySQLCursor

default_disclaimer = {
    'en': 'These subtitles have been generated automatically',
    'fr': 'Ces sous-titres ont été générés automatiquement',
    'de': 'Diese Untertitel wurden automatisch generiert',
    'it': 'Questi sottotitoli sono stati generati automaticamente'
}

default_missing_transcript = {
    'en': 'No transcripts available for this video',
    'fr': 'Pas de sous-titres disponibles pour cette video'
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


def prepare_values_mysql_with_types(values, types: list[Literal["str", "int", "float"]], encoding='utf8'):
    values_str = []
    assert len(values) == len(types)
    for val, val_type in zip(values, types):
        if val is None:
            values_str.append("NULL")
        elif val_type == "str":
            val_str = str(val).encode(encoding, errors='ignore').decode(encoding)
            values_str.append(f"'" + val_str.replace("\\", "\\\\").replace("'", "\\'").replace(";", "\\;") + "'")
        elif val_type == "int":
            values_str.append(str(int(val)))
        elif val_type == "float":
            val_float = float(val)
            if isnan(val_float) or isinf(val_float):
                values_str.append("NULL")
            else:
                values_str.append(str(float(val)))
        else:
            raise ValueError('types must be a list of either "str", "int" or "float".')
    return values_str


def insert_line_into_table_with_types(cursor, schema, table_name, columns, values, types, encoding='utf8', retry=5):
    values_str = prepare_values_mysql_with_types(values, types, encoding=encoding)
    sql_query = f"""
                INSERT INTO `{schema}`.`{table_name}` ({', '.join(columns)})
                VALUES ({', '.join(values_str)});
            """
    execute_query(cursor, sql_query, retry=retry)


def execute_query(cursor: MySQLCursor, sql_query, retry=5):
    try:
        cursor.execute(sql_query)
    except Exception as e:
        msg = 'Received exception: ' + str(e) + '\m'
        if retry > 0:
            msg += f"Trying to reconnect and resend the query ({retry}x at most)"
            status_msg(msg, sections=['MYSQL INSERT', 'WARNING'], color='grey')
            get_connection(cursor).ping(reconnect=True)
            execute_query(cursor, sql_query, retry=retry-1)
        else:
            msg += f"No more tries left to execute the query:\n" + sql_query
            status_msg(msg, sections=['MYSQL INSERT', 'ERROR'], color='red')
            raise e


def get_connection(cursor):
    if isinstance(cursor, MySQLCursor):
        return cursor._connection
    elif isinstance(cursor, CMySQLCursor):
        return cursor._cnx
    else:
        raise NotImplementedError(f'cannot get connection from cursor of type {type(cursor)}')



def convert_subtitle_into_segments(caption_data, file_ext='srt', text_key='text'):
    caption_lines = caption_data.encode('utf8').decode('utf-8-sig', errors='ignore').split('\n')
    time1_regexp = r'(:?(:?(?P<h1>[\d]{1,2}):)?(?P<m1>[\d]{1,2}):)?(?P<s1>[\d]{1,2})(:?[.,](?P<subs1>[\d]{0,6}))?'
    time2_regexp = r'(:?(:?(?P<h2>[\d]{1,2}):)?(?P<m2>[\d]{1,2}):)?(?P<s2>[\d]{1,2})(:?[.,](?P<subs2>[\d]{0,6}))?'
    time_regexp = compile(r'[\s]*' + time1_regexp + r'[\s]*-->[\s]*' + time2_regexp + r'[\s]*')
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
            if not line.strip():
                continue
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


def combine_language_segments(text_key='text', precision_s=0.5, **kwargs):
    segments_combined = []
    languages = list(kwargs.keys())
    n_segments = len(kwargs[languages[0]])
    for lang in languages[1:]:
        if len(kwargs[lang]) != n_segments:
            segments_combined = harmonize_segments(text_key='text', precision_s=precision_s, **kwargs)
            return segments_combined
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
                if not -precision_s < segment['start'] - segment_equiv['start'] < precision_s or \
                        not -precision_s < segment['end'] - segment_equiv['end'] < precision_s:
                    raise ValueError(f'{seg_idx}th segment timing are not the same for {languages[0]} and {lang}')
                segments_combined[seg_idx][lang] = segment[text_key]
    return segments_combined


def harmonize_segments(precision_s=0.5, text_key='text', **kwargs):
    harmonized_segments_interval, link_to_harmonized_segments = _harmonize_segments_interval(precision_s=precision_s, **kwargs)
    harmonized_segments = [
        {'id': idx, 'start': start, 'end': end, }
        for idx, (start, end) in enumerate(harmonized_segments_interval)
    ]
    for lang, segments_lang in kwargs.items():
        harm_seg_idx = 0
        for lang_seg_idx, segment in enumerate(segments_lang):
            harmonized_segments_linked = []
            while harm_seg_idx < len(harmonized_segments) and link_to_harmonized_segments[harm_seg_idx][lang] == lang_seg_idx:
                harmonized_segments_linked.append(harm_seg_idx)
                harm_seg_idx += 1
            num_linked_segments = len(harmonized_segments_linked)
            if num_linked_segments == 0:
                pass
            elif num_linked_segments == 1:
                harmonized_segments[harmonized_segments_linked[0]][lang] = segment[text_key]
            else:
                intervals = [
                    (harmonized_segments_interval[s_idx][0], harmonized_segments_interval[s_idx][1])
                    for s_idx in harmonized_segments_linked
                ]
                split_text = _split_text_in_intervals(segment[text_key], intervals)
                for harm_seg_linked, text in zip(harmonized_segments_linked, split_text):
                    harmonized_segments[harm_seg_linked][lang] = text
    return harmonized_segments


def _split_text_in_intervals(text, split_intervals, split_characters=('\n', '.', ';', ',', ' ')):
    split_text = []
    split_characters = list(split_characters)
    if len(split_characters) > 0:
        char = split_characters.pop(0)
        for interval_or_list, split_text_partial in _split_text_in_intervals_partial(
                text, split_intervals, split_char=char
        ):
            if isinstance(interval_or_list, tuple):
                split_text.append(split_text_partial)
            else:
                split_text.extend(_split_text_in_intervals(split_text_partial, interval_or_list, split_characters))
    else:
        length_full = split_intervals[-1][1] - split_intervals[0][0]
        end_intervals_fraction = [
            float(interval[1] - split_intervals[0][0]) / length_full for interval in split_intervals[:-1]
        ]
        split_pos = [frac*len(text) for frac in end_intervals_fraction]
        text_split_in_middle = _split_text(text, split_pos)
        for idx, t in enumerate(text_split_in_middle):
            if idx < len(text_split_in_middle)-1:
                split_text.append(t + '-')
            else:
                split_text.append(t)
    return split_text


def _split_text_in_intervals_partial(text, split_intervals, split_char='\n'):
    text = text.strip()
    length_full = split_intervals[-1][1]-split_intervals[0][0]
    # we ignore last interval as its end will always be at a fraction=1
    end_intervals_fraction = [float(interval[1]-split_intervals[0][0])/length_full for interval in split_intervals[:-1]]
    char_positions = [m.start() for m in finditer(split_char, text)]
    char_positions_fraction = [pos/len(text) for pos in char_positions]
    if len(char_positions) == len(end_intervals_fraction):
        return zip(split_intervals, _split_text(text, char_positions))
    elif len(char_positions) > len(end_intervals_fraction):
        index_split_char_kept = _get_index_closest_fractions(char_positions_fraction, end_intervals_fraction)
        char_positions_kept = [char_positions[idx] for idx in index_split_char_kept]
        return zip(split_intervals, _split_text(text, char_positions_kept))
    else:  # not enough split_char
        split_text = _split_text(text, char_positions)
        indices_interval_split = _get_index_closest_fractions(end_intervals_fraction, char_positions_fraction)
        index_current_interval = 0
        index_current_split_text = 0
        returned_intervals_and_text = []
        for index_interval_split in indices_interval_split:
            if index_current_interval == index_interval_split:
                returned_intervals_and_text.append(
                    (split_intervals[index_current_interval], split_text[index_current_split_text])
                )
            else:
                intervals_current_split_text = []
                while index_current_interval <= index_interval_split:
                    intervals_current_split_text.append(split_intervals[index_current_interval])
                    index_current_interval += 1
                returned_intervals_and_text.append((intervals_current_split_text, split_text[index_current_split_text]))
            index_current_interval += 1
            index_current_split_text += 1
        if index_current_interval == len(split_intervals) - 1:
            returned_intervals_and_text.append(
                (split_intervals[index_current_interval], split_text[index_current_split_text])
            )
        else:
            intervals_current_split_text = []
            while index_current_interval <= len(split_intervals) - 1:
                intervals_current_split_text.append(split_intervals[index_current_interval])
                index_current_interval += 1
            returned_intervals_and_text.append((intervals_current_split_text, split_text[index_current_split_text]))
        return returned_intervals_and_text


def _split_text(text, split_positions):
    if not split_positions:
        return [text]
    text_split = []
    for idx, pos in enumerate(split_positions):
        if idx == 0:
            text_split.append(text[:pos].strip())
        else:
            text_split.append(text[split_positions[idx-1]:pos].strip())
    text_split.append(text[split_positions[-1]:].strip())
    return text_split


def _get_index_closest_fractions(source_fractions, target_fractions):
    """
    Return the index of the fractions inside source fraction which are the closest to target fraction.
    The length of source_fractions must be larger or equal to the length of target fractions.
    The returned list is the same size as target fraction.
    """

    def _get_difference(sources: list, targets: list, skip_source: list):
        sources_with_skip = sources.copy()
        for idx in reversed(skip_source):
            sources_with_skip.pop(idx)
        assert len(sources_with_skip) == len(targets)
        return sum([abs(s_f - t_f) for s_f, t_f in zip(sources_with_skip, targets)])

    def _get_possible_skips(length_source, num_skip, start=0):
        if num_skip == 1:
            return [(idx,) for idx in range(start, length_source)]
        possible_skips = []
        for first_skip in range(start, length_source-num_skip+1):
            for possible_skips_given_first_skip in _get_possible_skips(length_source, num_skip-1, start=first_skip+1):
                possible_skips.append((first_skip,) + possible_skips_given_first_skip)
        return possible_skips

    n_skip = len(source_fractions) - len(target_fractions)
    assert n_skip >= 0
    if n_skip == 0:
        return list(range(len(source_fractions)))
    min_difference = None
    skips_with_min_difference = None
    for skips in _get_possible_skips(len(source_fractions), n_skip):
        difference = _get_difference(source_fractions, target_fractions, skips)
        if min_difference is None or difference < min_difference:
            min_difference = difference
            skips_with_min_difference = skips
    return [idx for idx in range(len(source_fractions)) if idx not in skips_with_min_difference]


def _harmonize_segments_interval(precision_s=0.5, **kwargs):
    harmonized_segments = []
    link_to_language_segments = []
    for lang, segments in kwargs.items():
        harmonized_segment_idx = 0
        for seg_idx in range(len(segments)):
            seg = segments[seg_idx]
            if harmonized_segment_idx > len(harmonized_segments) - 1:
                harmonized_segments.append((seg['start'], seg['end']))
                link_to_language_segments.append({lang: seg_idx})
                harmonized_segment_idx += 1
            else:
                if not -precision_s < harmonized_segments[harmonized_segment_idx][0] - seg['start'] < precision_s:
                    status_msg(
                        f"start of {lang} segment {seg_idx} ({seg['start']}) did not match the harmonized segment "
                        f"{harmonized_segment_idx} ({harmonized_segments[harmonized_segment_idx][0]})",
                        color='yellow', sections=['KALTURA', 'HARMONIZE SEGMENTS', 'WARNING']
                    )
                    harmonized_segments[harmonized_segment_idx] = (
                        min(seg['start'], harmonized_segments[harmonized_segment_idx][0]),
                        harmonized_segments[harmonized_segment_idx][1]
                    )
                if -precision_s < harmonized_segments[harmonized_segment_idx][1] - seg['end'] < precision_s:
                    link_to_language_segments[harmonized_segment_idx][lang] = seg_idx
                    harmonized_segment_idx += 1
                else:
                    # the harmonized segment is larger than the new one
                    if harmonized_segments[harmonized_segment_idx][1] > seg['end']:
                        # the next segment start before the current harmonized segment end
                        if seg_idx + 1 < len(segments) and \
                                segments[seg_idx+1]['start'] < harmonized_segments[harmonized_segment_idx][1]:
                            current_harm_seg_end = harmonized_segments[harmonized_segment_idx][1]
                            harmonized_segments[harmonized_segment_idx] = (
                                harmonized_segments[harmonized_segment_idx][0], seg['end']
                            )
                            harmonized_segments.insert(
                                harmonized_segment_idx + 1, (segments[seg_idx + 1]['start'], current_harm_seg_end)
                            )
                            link_to_language_segments.insert(
                                harmonized_segment_idx + 1, link_to_language_segments[harmonized_segment_idx].copy()
                            )
                        else:
                            if not -precision_s < harmonized_segments[harmonized_segment_idx][1]-seg['end'] < precision_s:
                                status_msg(
                                    f'end time of {lang} segment {seg_idx} do not match the harmonized segment '
                                    f"{harmonized_segment_idx} ({harmonized_segments[harmonized_segment_idx][1]})",
                                    color='yellow', sections=['KALTURA', 'HARMONIZE SEGMENTS', 'WARNING']
                                )
                        link_to_language_segments[harmonized_segment_idx][lang] = seg_idx
                        harmonized_segment_idx += 1
                    else:  # the harmonized segment is smaller than the new one
                        found_matching_end = False
                        while harmonized_segment_idx+1 < len(harmonized_segments) and (
                                seg_idx + 1 >= len(segments) or
                                harmonized_segments[harmonized_segment_idx+1][1] <= segments[seg_idx+1]['start']
                            ):
                            link_to_language_segments[harmonized_segment_idx][lang] = seg_idx
                            harmonized_segment_idx += 1
                            if -precision_s < harmonized_segments[harmonized_segment_idx][1]-seg['end'] < precision_s:
                                found_matching_end = True
                                break
                        if not found_matching_end:
                            status_msg(
                                f'end time of {lang} segment {seg_idx} do not match the harmonized segment '
                                f"{harmonized_segment_idx} ({harmonized_segments[harmonized_segment_idx][1]})",
                                color='yellow', sections=['KALTURA', 'HARMONIZE SEGMENTS', 'WARNING']
                            )
                            harmonized_segments[harmonized_segment_idx] = (
                                harmonized_segments[harmonized_segment_idx][0],
                                max(seg['end'], harmonized_segments[harmonized_segment_idx][1])
                            )
                        link_to_language_segments[harmonized_segment_idx][lang] = seg_idx
                        harmonized_segment_idx += 1
    return harmonized_segments, link_to_language_segments


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
