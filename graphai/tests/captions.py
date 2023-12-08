import unittest
from os.path import join, dirname

test_files_dir = join(dirname(__file__), 'test_files')
piper_mysql_json_file = join(dirname(dirname(__file__)), 'config', 'piper_db.json')


class Captions(unittest.TestCase):
    def test_convert_and_combine(self):
        from graphai.utils import convert_caption_data_into_segments, combine_language_segments

        with open(join(test_files_dir, '0_00w4rf3f_en.srt')) as fid:
            data_en = fid.read()
        with open(join(test_files_dir, '0_00w4rf3f_fr.srt')) as fid:
            data_fr = fid.read()

        seg_fr = convert_caption_data_into_segments(data_fr, file_ext='srt')
        seg_en = convert_caption_data_into_segments(data_en, file_ext='srt')
        captions = combine_language_segments(en=seg_en, fr=seg_fr)
        self.assertIsNotNone(captions)
        return captions

    def test_get_caption_from_kaltura(self):
        from graphai.rcp import get_subtitles_from_kaltura

        subtitles_from_kaltura = get_subtitles_from_kaltura('0_00w4rf3f', piper_mysql_json_file=piper_mysql_json_file)
        subtitles_from_files = self.test_convert_and_combine()
        self.assertListEqual(subtitles_from_kaltura, subtitles_from_files)


if __name__ == '__main__':
    unittest.main()
