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


if __name__ == '__main__':
    unittest.main()
