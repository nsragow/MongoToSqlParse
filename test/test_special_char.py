import sqlite3 as sql
import unittest

from Main import mongo_to_sql
from test.List2DToInsert import to_sql_insert

SPECIAL_CHARS = [
    '\\',
    '/',
    ',',
    '{',
    '}',
    '[',
    ']',
    '(',
    ')',
    ':',
]


class SpecialCharTestCase(unittest.TestCase):
    @classmethod
    def populate_sql(cls):
        c = cls.conn.cursor()
        c.execute("CREATE TABLE special (val text)")
        cls.records = []
        for special_char in SPECIAL_CHARS:
            cls.records.append(tuple([special_char]))
        sql_insert = to_sql_insert(cls.records)
        sql_insert_q = f"INSERT INTO special VALUES {sql_insert}"
        c.execute(sql_insert_q)
        cls.conn.commit()

    @classmethod
    def setUpClass(cls) -> None:
        cls.conn = sql.connect(':memory:')
        cls.populate_sql()

    def setUp(self) -> None:
        self.c = SpecialCharTestCase.conn.cursor()

    def tearDown(self) -> None:
        SpecialCharTestCase.conn.commit()

    @property
    def records(self):
        return SpecialCharTestCase.records

    def query(self, mongo):
        return list(self.c.execute(mongo_to_sql(mongo)))

    def case_iter(self, cases):
        for i in range(len(cases)):
            with self.subTest(i=i):
                result = self.query(cases[i][0])
                if len(cases[i]) == 3:
                    self.assertEqual(result, cases[i][1], cases[i][2])
                else:
                    self.assertEqual(result, cases[i][1])

    def test_double_quote(self):
        cases = []
        for special_char in SPECIAL_CHARS:
            cases.append(('db.special.find({val: "' + special_char + '"})', list(filter(lambda r: r[0] == special_char, self.records)), f'Testing char "{special_char}"'))
        self.case_iter(cases)

    def test_single_quote(self):
        cases = []
        for special_char in SPECIAL_CHARS:
            cases.append(('db.special.find({val: \'' + special_char + '\'})', list(filter(lambda r: r[0] == special_char, self.records)), f'Testing char "{special_char}"'))
        self.case_iter(cases)

    def test_in(self):
        self.case_iter([
            ('db.special.find({val: {$in: [ "," , ":" , "}" ] }})', list(filter(lambda r: r[0] in [",",":","}"], self.records))),
            ('db.special.find({val: {$in: [",",":","}"] }})', list(filter(lambda r: r[0] in [",",":","}"], self.records))),
            ('db.special.find({val: {$in: [ "{" , "[" , "]" ] }})', list(filter(lambda r: r[0] in ["{","[","]"], self.records))),
            ('db.special.find({val: {$in: ["{","[","]"] }})', list(filter(lambda r: r[0] in ["{","[","]"], self.records))),
        ])

    def test_nested(self):
        self.case_iter([
            ('db.special.find({$or: [ {val: ","}, {val: ":"}, {val: "}"} ] })', list(filter(lambda r: r[0] in [",",":","}"], self.records))),
            ('db.special.find({$or: [ {val: "{"}, {val: "["}, {val: "]"} ] })', list(filter(lambda r: r[0] in ["{","[","]"], self.records))),
            ('db.special.find({$and: [ {val: "{"}, {val: "{"} ] })', list(filter(lambda r: r[0] in ["{"], self.records))),
            ('db.special.find({$and: [ {val: "}"}, {val: "}"} ] })', list(filter(lambda r: r[0] in ["}"], self.records))),
        ])
