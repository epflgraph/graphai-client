import unittest
from graphai_client.client_api.utils import login

login_info = login()


class ConceptExtractionTests(unittest.TestCase):
    def test_extract_concepts_from_text(self):
        from graphai_client.client_api.text import extract_concepts_from_text

        concepts_and_scores = extract_concepts_from_text(
            'a demonstration of a theorem must contain a proof', login_info
        )
        concepts_and_scores_ordered = sorted(concepts_and_scores, key=lambda x: x['LevenshteinScore'], reverse=True)
        self.assertEqual(concepts_and_scores_ordered[0]['PageTitle'], 'Theorem')

        self.assertEqual(extract_concepts_from_text('', login_info), [])

    def test_login_after_error_401(self):
        from graphai_client.client_api.text import extract_concepts_from_text

        login_info['token'] = 'wrong_token'
        extract_concepts_from_text('a demonstration of a theorem must contain a proof', login_info)
        self.assertNotEqual(login_info['token'], 'wrong_token')


if __name__ == '__main__':
    unittest.main()
