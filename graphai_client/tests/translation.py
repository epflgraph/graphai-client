import unittest
from graphai_client.client_api.utils import login

login_info = login()


class TranslationTests(unittest.TestCase):
    def test_translate_simple_text(self):
        from graphai_client.client_api.translation import translate_text

        translated_text = translate_text(
            'A demonstration of a theorem must contain a proof.', 'en', 'fr', login_info, force=True
        )
        self.assertEqual(translated_text.strip(), 'Une démonstration d\'un théorème doit contenir une preuve.')

    def test_translate_simple_list(self):
        from graphai_client.client_api.translation import translate_text

        translated_text = translate_text(
            ['A demonstration of a theorem must contain a proof.'], 'en', 'fr', login_info
        )
        self.assertEqual(translated_text[0].strip(), 'Une démonstration d\'un théorème doit contenir une preuve.')

    def test_translate_list_with_none(self):
        from graphai_client.client_api.translation import translate_text

        translated_text = translate_text(
            [None, 'A demonstration of a theorem must contain a proof.', ''], 'en', 'fr', login_info
        )
        self.assertEqual(translated_text[0], None)
        self.assertEqual(translated_text[2], None)

    def test_detect_language(self):
        from graphai_client.client_api.translation import detect_language

        self.assertEqual(detect_language('A demonstration of a theorem must contain a proof.', login_info), 'en')
        self.assertEqual(
            detect_language('Une démonstration d\'un théorème doit contenir une preuve.', login_info),
            'fr'
        )

    def test_detect_language_fail(self):
        from graphai_client.client_api.translation import detect_language

        self.assertEqual(detect_language('https://www.linkedin.com/', login_info, quiet=True), None)


class TestSplitText(unittest.TestCase):
    def test_split(self):
        from graphai_client.client_api.translation import split_text

        self.assertListEqual(split_text('abcdef\nghijkl', 10), ['abcdef\n', 'ghijkl'])
        self.assertListEqual(split_text('abc\ndef\nghijkl', 10), ['abc\ndef\n', 'ghijkl'])
        self.assertListEqual(split_text('abcdef.\nghijkl', 10), ['abcdef.\n', 'ghijkl'])
        self.assertListEqual(split_text('abcdef.\nghijkl', 10, split_characters=('.', '\n')), ['abcdef.', '\nghijkl'])
        self.assertListEqual(split_text('abcdefghijkl', 10), ['abcdefghij', 'kl'])


if __name__ == '__main__':
    unittest.main()
