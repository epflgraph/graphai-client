import unittest
from os.path import join, dirname
from graphai_client.client_api.utils import login

test_files_dir = join(dirname(__file__), 'test_files')
login_info = login()


class Captions(unittest.TestCase):
    def test_convert_and_combine(self):
        from graphai_client.utils import convert_subtitle_into_segments, combine_language_segments

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
        from graphai_client.rcp import get_subtitles_from_kaltura

        subtitles_from_kaltura = get_subtitles_from_kaltura('0_00w4rf3f', login_info)
        subtitles_from_files = self.test_convert_and_combine()
        self.assertListEqual(subtitles_from_kaltura, subtitles_from_files)

        # video with a caption "No transcripts available for this video"
        subtitles_from_kaltura = get_subtitles_from_kaltura('0_4swj44wd', login_info)
        self.assertEqual(subtitles_from_kaltura, None)

    def test_initial_disclaimer_new_segment(self):
        from graphai_client.utils import add_initial_disclaimer, default_disclaimer

        subtitles_from_files = self.test_convert_and_combine()
        subtitles_with_disclaimer = add_initial_disclaimer(subtitles_from_files, default_disclaimer)
        self.assertEqual(subtitles_with_disclaimer[0]['id'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['start'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['end'], 2)
        for lang in ('en', 'fr'):
            self.assertEqual(subtitles_with_disclaimer[0][lang], default_disclaimer[lang])
            self.assertEqual(subtitles_with_disclaimer[1][lang], subtitles_from_files[0][lang])

    def test_initial_disclaimer_first_segment_starting_at_0(self):
        from graphai_client.utils import add_initial_disclaimer, default_disclaimer
        from graphai_client.rcp import get_subtitles_from_kaltura

        subtitles_from_kaltura = get_subtitles_from_kaltura('0_bgyh2jg1', login_info, destination_languages=None)
        subtitles_with_disclaimer = add_initial_disclaimer(subtitles_from_kaltura)

        self.assertEqual(subtitles_with_disclaimer[0]['id'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['start'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['end'], subtitles_from_kaltura[0]['end'])
        self.assertEqual(subtitles_with_disclaimer[0]['en'].split('\n')[0], default_disclaimer['en'])

    def test_initial_disclaimer_first_segment_starting_at_less_than_2s(self):
        from graphai_client.utils import add_initial_disclaimer, default_disclaimer
        from graphai_client.rcp import get_subtitles_from_kaltura

        disclaimer_per_language = {
            'en': 'These subtitles have been generated automatically'
        }
        subtitles_from_kaltura = get_subtitles_from_kaltura('0_oo8itzlf', login_info, destination_languages=None)
        subtitles_with_disclaimer = add_initial_disclaimer(subtitles_from_kaltura, disclaimer_per_language)

        self.assertEqual(subtitles_with_disclaimer[0]['id'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['start'], 0)
        self.assertEqual(subtitles_with_disclaimer[0]['end'], subtitles_from_kaltura[0]['end'])
        self.assertEqual(subtitles_with_disclaimer[0]['en'].split('\n')[0], default_disclaimer['en'])

    def test_initial_disclaimer_with_empty_lang(self):
        from graphai_client.utils import add_initial_disclaimer, default_disclaimer

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
        from graphai_client.utils import _harmonize_segments_interval

        interval, links = _harmonize_segments_interval(
            precision_s=0.01,
            fr=[{'start': 0, 'end': 10}],
            en=[{'start': 0, 'end': 5}, {'start': 6, 'end': 8}, {'start': 8, 'end': 10}],
        )
        self.assertListEqual(interval, [(0, 5), (6, 8), (8, 10)])
        self.assertListEqual(links, [{'fr': 0, 'en': 0}, {'fr': 0, 'en': 1}, {'fr': 0, 'en': 2}])

    def test_split_text_in_intervals(self):
        from graphai_client.utils import _split_text_in_intervals

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
        from graphai_client.utils import _get_index_closest_fractions

        self.assertListEqual(_get_index_closest_fractions([0.1, 0.4, 0.9], [0.3, 0.6, 0.9]), [0, 1, 2])
        self.assertListEqual(_get_index_closest_fractions([0.1, 0.4, 0.7, 0.9], [0.3, 0.6, 0.9]), [1, 2, 3])
        self.assertListEqual(_get_index_closest_fractions([0.1, 0.33, 0.4, 0.55, 0.7, 0.9], [0.3, 0.6, 0.9]), [1, 3, 5])

    def test_harmonize_segments(self):
        from graphai_client.utils import convert_subtitle_into_segments, harmonize_segments

        segments_to_test = [
            {'en': '0_iz3wgt1s_en.srt', 'fr': '0_iz3wgt1s_fr.srt'},
            {'en': '0_vvgduz0b_en.srt', 'fr': '0_vvgduz0b_fr.srt'},
        ]
        for test_dict in segments_to_test:
            segments_per_languages = {}
            for lang, filename in test_dict.items():
                with open(join(test_files_dir, filename)) as fid:
                    subtitles = fid.read()
                segments_per_languages[lang] = convert_subtitle_into_segments(subtitles, file_ext='srt')

            segments = harmonize_segments(**segments_per_languages, precision_s=2)
            self.assertIsNotNone(segments)


if __name__ == '__main__':
    unittest.main()
