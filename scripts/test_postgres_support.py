"""Guard the support inventory and independent scalar boundary oracle."""
import copy
import json
import pathlib
import tempfile
import unittest
from decimal import Decimal
from postgres_support_harness import evaluate, load_matrix


class SupportMatrixTests(unittest.TestCase):
    def test_references_and_declared_signatures_are_complete(self):
        matrix = load_matrix()
        self.assertEqual(len(matrix['scalar_functions']), 52)
        self.assertEqual({case['oid'] for case in matrix['source_rejections']},
                         set(matrix['unsupported_source_type_oids']))
        self.assertTrue(set(matrix['source_type_oids']).isdisjoint(matrix['unsupported_source_type_oids']))
        for case in matrix['scalar_functions'] + matrix['coercion_cases']:
            row = dict.fromkeys(['category', 'group_id', 'small_value', 'large_value', 'codepoint', 'vartext', 'amount', 'on_date', 'span', 'local_time', 'event_time'])
            self.assertIsNone(evaluate(case, row), case)

    def test_duplicate_signature_and_missing_qualification_fail(self):
        original = load_matrix()
        invalid = copy.deepcopy(original)
        invalid['scalar_functions'].append(invalid['scalar_functions'][0])
        self.reject(invalid, 'unique')
        invalid = copy.deepcopy(original)
        invalid['features'][0]['qualification'] = ['missing-qualification.py']
        self.reject(invalid, 'Missing support-matrix reference')

    def test_oracle_arguments_and_rejection_diagnostics_are_required(self):
        invalid = load_matrix()
        invalid['scalar_functions'][0]['arguments'] = []
        self.reject(invalid, 'one oracle argument')
        invalid = load_matrix()
        invalid['rejections'][0]['error_contains'] = ''
        self.reject(invalid, 'diagnostic')

    def reject(self, matrix, message):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / 'matrix.json'
            path.write_text(json.dumps(matrix))
            with self.assertRaisesRegex(ValueError, message):
                load_matrix(path)

    def test_unicode_duplicate_translation_and_signed_widths(self):
        def run(name, values, oids):
            return evaluate({'name': name, 'input_oids': oids,
                             'arguments': [{'literal': value} for value in values]}, {})
        self.assertEqual(run('translate', ['é🙂éx', 'é🙂é', 'abz'], [25, 25, 25]), 'abax')
        self.assertEqual(run('substr', ['é🙂xy', -1, 4], [25, 23, 23]), 'é🙂')
        self.assertEqual(run('right', ['é🙂xy', -2], [25, 23]), 'xy')
        self.assertEqual(run('left', ['é🙂xy', -2], [25, 23]), 'é🙂')
        self.assertEqual(run('to_hex', [-1], [23]), 'ffffffff')
        self.assertEqual(run('to_hex', [-1], [20]), 'ffffffffffffffff')
        self.assertEqual(run('octet_length', ['é🙂'], [25]), 6)
        self.assertEqual(run('split_part', ['é🙂x🙂y', '🙂', -2], [25, 25, 23]), 'x')
        self.assertEqual(run('replace', ['é', '', 'x'], [25, 25, 25]), 'é')

    def test_numeric_rounding_oracle_retains_sign_and_special_values(self):
        def run(name, values, oids):
            return evaluate({'name': name, 'input_oids': oids,
                             'arguments': [{'literal': value} for value in values]}, {})
        self.assertEqual(run('round', [Decimal('2.5')], [1700]), Decimal(3))
        self.assertEqual(run('round', [Decimal('-1250'), -2], [1700,23]), Decimal(-1300))
        self.assertEqual(run('trunc', [Decimal('-1250'), -2], [1700,23]), Decimal(-1200))
        self.assertEqual(run('ceil', [Decimal('-2.5')], [1700]), Decimal(-2))
        self.assertEqual(run('floor', [Decimal('-2.5')], [1700]), Decimal(-3))
        self.assertEqual(run('abs', [Decimal('-Infinity')], [1700]), 'Infinity')
        self.assertEqual(run('round', [Decimal('NaN')], [1700]), 'NaN')
        self.assertIsNone(run('round', [None], [1700]))


if __name__ == '__main__':
    unittest.main()
