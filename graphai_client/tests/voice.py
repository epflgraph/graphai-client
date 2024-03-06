import unittest
from graphai_client.client_api.utils import login

login_info = login()
kaltura_url_template = 'https://api.cast.switch.ch/p/113/sp/11300/playManifest/entryId/{}' + \
                           '/format/download/protocol/https/flavorParamIds/0'


class NoVoice(unittest.TestCase):
    ids_without_audio = ['0_i8zqj20g', '0_0eai1akl', '0_0fgr0ugf']
    ids_with_music_only = ['0_e9w2yhoe', '0_191runee', '0_0edfzg38', '0_u1offnl2']

    def test_transcription_no_audio(self):
        self._test_transcription_no_output(self.ids_without_audio)

    def test_language_detection_no_audio(self):
        self._test_language_detection_no_output(self.ids_without_audio)

    def test_transcription_music_only(self):
        self._test_transcription_no_output(self.ids_with_music_only)

    def test_language_detection_music_only(self):
        self._test_language_detection_no_output(self.ids_with_music_only)

    def _test_transcription_no_output(self, kaltura_ids):
        from graphai_client.client import process_video

        for kaltura_id in kaltura_ids:
            url = kaltura_url_template.format(kaltura_id)
            video_info = process_video(url, force=True, analyze_slides=False)
            self.assertIsNone(video_info['audio_language'])
            self.assertListEqual(video_info['subtitles'], [])

    def _test_language_detection_no_output(self, kaltura_ids):
        from graphai_client.client import process_video

        for kaltura_id in kaltura_ids:
            url = kaltura_url_template.format(kaltura_id)
            video_info = process_video(
                url, force=True, analyze_slides=False, analyze_audio=False, detect_audio_language=True
            )
            self.assertIsNone(video_info['audio_language'])
            self.assertListEqual(video_info['subtitles'], [])


class Voice(unittest.TestCase):
    ids_for_lang = {
        'it': ['0_02lndfvg'],
        'de': ['0_0gn6xnrf'],
    }

    def test_language_detection(self):
        from graphai_client.client import process_video

        for lang, kaltura_ids in self.ids_for_lang.items():
            for kaltura_id in kaltura_ids:
                url = kaltura_url_template.format(kaltura_id)
                video_info = process_video(
                    url, force=True, analyze_slides=False, analyze_audio=False, detect_audio_language=True
                )
                self.assertEqual(video_info['audio_language'], lang)


if __name__ == '__main__':
    unittest.main()
