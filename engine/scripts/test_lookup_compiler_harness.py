import unittest
from lookup_compiler_harness import quoted_left


class LookupHarnessTests(unittest.TestCase):
    def test_quoted_alias_keeps_owned_schema_name(self):
        query = 'SELECT l.id FROM owned_sql.auction l WHERE l.id>0'
        self.assertEqual(quoted_left(query), 'SELECT "L".id FROM owned_sql.auction AS "L" WHERE "L".id>0')


if __name__ == '__main__':
    unittest.main()
