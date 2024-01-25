import unittest
from os.path import join, dirname

test_files_dir = join(dirname(__file__), 'test_files')
piper_mysql_json_file = join(dirname(dirname(__file__)), 'config', 'piper_db.json')


class Captions(unittest.TestCase):
    def test_convert_and_combine(self):
        from graphai.utils import convert_subtitle_into_segments, combine_language_segments

        with open(join(test_files_dir, '0_00w4rf3f_en.srt')) as fid:
            data_en = fid.read()
        with open(join(test_files_dir, '0_00w4rf3f_fr.srt')) as fid:
            data_fr = fid.read()

        seg_fr = convert_subtitle_into_segments(data_fr, file_ext='srt')
        seg_en = convert_subtitle_into_segments(data_en, file_ext='srt')
        captions = combine_language_segments(en=seg_en, fr=seg_fr)
        self.assertIsNotNone(captions)
        return captions

    def test_get_caption_from_kaltura(self):
        from graphai.rcp import get_subtitles_from_kaltura

        subtitles_from_kaltura = get_subtitles_from_kaltura('0_00w4rf3f', piper_mysql_json_file=piper_mysql_json_file)
        subtitles_from_files = self.test_convert_and_combine()
        self.assertListEqual(subtitles_from_kaltura, subtitles_from_files)

    def test_initial_disclaimer_new_segment(self):
        from graphai.utils import add_initial_disclaimer, default_disclaimer

        subtitles_from_files = self.test_convert_and_combine()
        subtitles_with_disclaimer = add_initial_disclaimer(subtitles_from_files, default_disclaimer)
        self.assertEqual(subtitles_with_disclaimer[0]['id'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['start'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['end'], 2)
        for lang in ('en', 'fr'):
            self.assertEqual(subtitles_with_disclaimer[0][lang], default_disclaimer[lang])
            self.assertEqual(subtitles_with_disclaimer[1][lang], subtitles_from_files[0][lang])

    def test_initial_disclaimer_first_segment_starting_at_0(self):
        from graphai.utils import add_initial_disclaimer, default_disclaimer
        from graphai.rcp import get_subtitles_from_kaltura

        subtitles_from_kaltura = get_subtitles_from_kaltura(
            '0_bgyh2jg1', piper_mysql_json_file=piper_mysql_json_file, destination_languages=None
        )
        subtitles_with_disclaimer = add_initial_disclaimer(subtitles_from_kaltura)

        self.assertEqual(subtitles_with_disclaimer[0]['id'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['start'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['end'], subtitles_from_kaltura[0]['end'])
        self.assertEqual(subtitles_with_disclaimer[0]['en'].split('\n')[0], default_disclaimer['en'])

    def test_initial_disclaimer_first_segment_starting_at_less_than_2s(self):
        from graphai.utils import add_initial_disclaimer, default_disclaimer
        from graphai.rcp import get_subtitles_from_kaltura

        disclaimer_per_language = {
            'en': 'These subtitles have been generated automatically'
        }
        subtitles_from_kaltura = get_subtitles_from_kaltura(
            '0_oo8itzlf', piper_mysql_json_file=piper_mysql_json_file, destination_languages=None
        )
        subtitles_with_disclaimer = add_initial_disclaimer(subtitles_from_kaltura, disclaimer_per_language)

        self.assertEqual(subtitles_with_disclaimer[0]['id'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['start'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['end'], subtitles_from_kaltura[0]['end'])
        self.assertEqual(subtitles_with_disclaimer[0]['en'].split('\n')[0], default_disclaimer['en'])

    def test_initial_disclaimer_with_empty_lang(self):
        from graphai.utils import add_initial_disclaimer, default_disclaimer

        test_subtitles = [
            {'id': 0, 'start': 0, 'end': 2, 'fr': None, 'en': None, 'it': 'test'},
            {'id': 1, 'start': 0, 'end': 2, 'fr': None, 'en': 'test', 'it': 'test'},
            {'id': 2, 'start': 0, 'end': 2, 'fr': None, 'en': None, 'it': 'test'},
        ]
        subtitles_with_disclaimer = add_initial_disclaimer(test_subtitles, default_disclaimer)
        self.assertEqual(subtitles_with_disclaimer[0]['id'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['start'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['end'], 2)
        self.assertEqual(subtitles_with_disclaimer[0]['fr'], None)
        self.assertEqual(subtitles_with_disclaimer[0]['en'], default_disclaimer['en'])
        self.assertEqual(subtitles_with_disclaimer[0]['it'].split('\n')[0], default_disclaimer['it'])

    def test_harmonize_segment_interval(self):
        from graphai.utils import _harmonize_segments_interval

        starts, ends = _harmonize_segments_interval(
            precision_s=0.01, fr=[{'start': 0, 'end': 10}],
            en=[{'start': 0, 'end': 5}, {'start': 6, 'end': 8}, {'start': 8, 'end': 10}]
        )
        self.assertListEqual(starts, [0, 6, 8])
        self.assertListEqual(ends, [5, 8, 10])

    def test_split_text_in_intervals(self):
        from graphai.utils import _split_text_in_intervals

        segments_newlines = _split_text_in_intervals('abcde\nfgh\ni', [(0, 3), (3, 6), (6, 9)])
        self.assertListEqual(segments_newlines, ['abcde', 'fgh', 'i'])
        segments_many_newlines = _split_text_in_intervals('a\nbcd\ne\nfgh\ni', [(0, 3), (3, 6), (6, 9)])
        self.assertListEqual(segments_many_newlines, ['a\nbcd', 'e', 'fgh\ni'])
        segments_newline = _split_text_in_intervals('abcde\nfg hi', [(0, 3), (3, 6), (6, 9)])
        self.assertListEqual(segments_newline, ['abcde', 'fg', 'hi'])
        segments_space = _split_text_in_intervals('abc def ghi', [(0, 3), (3, 6), (6, 9)])
        self.assertListEqual(segments_space, ['abc', 'def', 'ghi'])
        segments_no_space = _split_text_in_intervals('abcdefghi', [(0, 3), (3, 6), (6, 9)])
        self.assertListEqual(segments_no_space, ['abc', 'def', 'ghi'])

    def test_get_closest_fractions(self):
        from graphai.utils import _get_index_closest_fractions

        self.assertListEqual(_get_index_closest_fractions([0.1, 0.4, 0.9], [0.3, 0.6, 0.9]), [0, 1, 2])
        self.assertListEqual(_get_index_closest_fractions([0.1, 0.4, 0.7, 0.9], [0.3, 0.6, 0.9]), [1, 2, 3])
        self.assertListEqual(_get_index_closest_fractions([0.1, 0.33, 0.4, 0.55, 0.7, 0.9], [0.3, 0.6, 0.9]), [1, 3, 5])

    def test_harmonize_segments(self):
        from graphai.utils import convert_subtitle_into_segments, harmonize_segments

        with open(join(test_files_dir, '0_vvgduz0b_en.srt')) as fid:
            data_en = fid.read()
        with open(join(test_files_dir, '0_vvgduz0b_fr.srt')) as fid:
            data_fr = fid.read()

        seg_fr = convert_subtitle_into_segments(data_fr, file_ext='srt')
        seg_en = convert_subtitle_into_segments(data_en, file_ext='srt')
        segments = harmonize_segments(fr=seg_fr, en=seg_en, precision_s=2)
        self.assertIsNotNone(segments)


if __name__ == '__main__':
    unittest.main()
