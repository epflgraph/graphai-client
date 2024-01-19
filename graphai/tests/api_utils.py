import unittest


class TestSplitText(unittest.TestCase):
    def test_split(self):
        from graphai.client_api.utils import split_text

        self.assertListEqual(split_text('abcdef\nghijkl', 10), ['abcdef\n', 'ghijkl'])
        self.assertListEqual(split_text('abc\ndef\nghijkl', 10), ['abc\ndef\n', 'ghijkl'])
        self.assertListEqual(split_text('abcdef.\nghijkl', 10), ['abcdef.\n', 'ghijkl'])
        self.assertListEqual(split_text('abcdef.\nghijkl', 10, split_characters=('.', '\n')), ['abcdef.', '\nghijkl'])
        self.assertListEqual(split_text('abcdefghijkl', 10), ['abcdefghij', 'kl'])


if __name__ == '__main__':
    unittest.main()
