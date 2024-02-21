import unittest
from graphai.client_api import login

login_info = login()


class ConceptExtractionTests(unittest.TestCase):
    def test_extract_concepts_from_text(self):
        from graphai.client_api.text import extract_concepts_from_text

        concepts_and_scores = extract_concepts_from_text(
            'a demonstration of a theorem must contain a proof', login_info
        )
        concepts_and_scores_ordered = sorted(concepts_and_scores, key=lambda x: x['LevenshteinScore'], reverse=True)
        self.assertEqual(concepts_and_scores_ordered[0]['PageTitle'], 'Theorem')

        self.assertEqual(extract_concepts_from_text('', login_info), [])


if __name__ == '__main__':
    unittest.main()
