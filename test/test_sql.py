import unittest

from SqlConstants import SQL_ALL
from mongopython_to_sql.DictToColumns import dict_to_columns, UNSUPPORTED_ERROR
from mongopython_to_sql.SqlFromDict import to_sql


class MyTestCase(unittest.TestCase):
    def test_columns(self):
        self.assertEqual(dict_to_columns({}), SQL_ALL)
        self.assertEqual(dict_to_columns(None), SQL_ALL)
        name = 'name'
        age = 'age'
        self.assertEqual(dict_to_columns({name: 1}), f'{name}')
        self.assertEqual(dict_to_columns({name: True}), f'{name}')
        try:
            dict_to_columns({name: 0})
            self.assertTrue(False, "should have thrown error")
        except AssertionError as e:
            self.assertEqual(e.args[0], UNSUPPORTED_ERROR)
        try:
            dict_to_columns({name: 1, age: False})
            self.assertTrue(False, "should have thrown error")
        except AssertionError as e:
            self.assertEqual(e.args[0], UNSUPPORTED_ERROR)
        val = dict_to_columns({name: 1, age: True})
        self.assertIn(val, (f'{name}, {age}', f'{age}, {name}'))

    def test_sql_from_dict(self):
        self.assertEqual(to_sql('users', {}, {}), 'SELECT * FROM users;')
        self.assertEqual(to_sql('users', {}, {'id': True}), 'SELECT id FROM users;')
        self.assertEqual(to_sql('users', {}, {'id': 1}), 'SELECT id FROM users;')
        self.assertIn(to_sql('users', {}, {'id': 1, 'key': 1}), ['SELECT id, key FROM users;','SELECT key, id FROM users;'])
        self.assertEqual(to_sql('users', {'id': 0}, {'id': 1}), 'SELECT id FROM users WHERE (id = 0);')
        self.assertEqual(to_sql('users', {'id': '0'}, {'id': 1}), 'SELECT id FROM users WHERE (id = \'0\');')
        self.assertEqual(to_sql('users', {'id': {'$gt': '100'}}, {'id': 1}), 'SELECT id FROM users WHERE (id > \'100\');')
        self.assertIn(to_sql('users', {'id': {'$gt': '100', '$lte': 1000}}, {'id': 1}), [
            'SELECT id FROM users WHERE (id > \'100\' AND id <= 1000);',
            'SELECT id FROM users WHERE (id <= 1000 AND id > \'100\');'
        ])

        self.assertIn(to_sql('users',
                      {'$or': [{'id': 100, 'key': 'test'}]},
                      None), [
                   'SELECT * FROM users WHERE ((id = 100) AND (key = \'test\'));',
                   'SELECT * FROM users WHERE ((key = \'test\') AND (id = 100));'
               ])

        self.assertIn(to_sql('users',
                      {'$or': [{'id': 100}, {'key': 'test'}]},
                      None), [
                   'SELECT * FROM users WHERE ((id = 100) OR (key = \'test\'));',
                   'SELECT * FROM users WHERE ((key = \'test\') OR (id = 100));'
               ])

        self.assertIn(to_sql('users',
                      {'$and': [{'id': 100}, {'key': 'test'}]},
                      None), [
                   'SELECT * FROM users WHERE ((id = 100) AND (key = \'test\'));',
                   'SELECT * FROM users WHERE ((key = \'test\') AND (id = 100));'
               ])
        val = to_sql('users',
                     {'$or': [{'id': 100}, {'key': 'test', 'key': {'$ne': 'nottest', '$gt': 100}}]},
                     None)
        self.assertTrue('(key != \'nottest\' AND key > 100)' in val or '(key > 100 AND key != \'nottest\')' in val)


if __name__ == '__main__':
    unittest.main()
