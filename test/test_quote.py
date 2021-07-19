# todo: Does mongo support escaping special characters? If so that needs to be supported
import sqlite3 as sql
import unittest

from Main import mongo_to_sql
from test.List2DToInsert import to_sql_insert


class QuoteTestCase(unittest.TestCase):
    @classmethod
    def populate_sql(cls):
        c = cls.conn.cursor()
        c.execute("CREATE TABLE quote (val text)")
        cls.records = [
            tuple(["'"]),
            tuple(['"']),
            tuple(['"""']),
            tuple(["'''"]),
        ]
        sql_single_quote = "''"
        sql_single_quote_3 = "''''''"
        sql_double_quote = '"'
        sql_double_quote_3 = '"""'
        sql_insert_q = f"INSERT INTO quote VALUES " \
                       f"('{ sql_single_quote }'), ('{ sql_double_quote }'), " \
                       f"('{sql_double_quote_3}'), ('{sql_single_quote_3}')"
        c.execute(sql_insert_q)
        cls.conn.commit()

    @classmethod
    def setUpClass(cls) -> None:
        cls.conn = sql.connect(':memory:')
        cls.populate_sql()

    def setUp(self) -> None:
        self.c = QuoteTestCase.conn.cursor()

    def tearDown(self) -> None:
        QuoteTestCase.conn.commit()

    @property
    def records(self):
        return QuoteTestCase.records

    def query(self, mongo):
        query_str = mongo_to_sql(mongo)
        return list(self.c.execute(query_str))

    def case_iter(self, cases):
        for i in range(len(cases)):
            with self.subTest(i=i):
                result = self.query(cases[i][0])
                if len(cases[i]) == 3:
                    self.assertEqual(result, cases[i][1], cases[i][2])
                else:
                    self.assertEqual(result, cases[i][1])

    def test_double(self):
        self.case_iter([
            ('db.quote.find({} )', list(filter(lambda r: True, self.records))),
            ('db.quote.find({val: \'"\' } )', list(filter(lambda r: r[0] == '"', self.records))),
            ('db.quote.find({val: \'"""\' } )', list(filter(lambda r: r[0] == '"""', self.records))),

            ('db.quote.find({val: \'"\',  val:\'"\'} )', list(filter(lambda r: r[0] == '"', self.records))),
            ('db.quote.find({val: {$in: [\'"\']}} )', list(filter(lambda r: r[0] == '"', self.records))),
            ('db.quote.find({val: {$ne: "\'"}} )', list(filter(lambda r: r[0] != "'", self.records))),
            ('db.quote.find({$or: [ {val: \'"\'}, {val: \'"\'} ] } )', list(filter(lambda r: r[0] == '"', self.records))),
            ('db.quote.find({$and: [{val: \'"\'}, {val: \'"\'}] })', list(filter(lambda r: r[0] == '"', self.records))),

        ])

    def test_single(self):
        self.case_iter([
            ('db.quote.find({} )', list(filter(lambda r: True, self.records))),
            ('db.quote.find({val: "\'" } )', list(filter(lambda r: r[0] == "'", self.records))),
            ('db.quote.find({val: "\'\'\'" } )', list(filter(lambda r: r[0] == "'''", self.records))),
            ('db.quote.find({val: {$ne : \'"\'} } )', list(filter(lambda r: r[0] != '"', self.records))),
            ('db.quote.find({val: "\'",  val:"\'"} )', list(filter(lambda r: r[0] == "'", self.records))),
            ('db.quote.find({val: {$in: ["\'"]}} )', list(filter(lambda r: r[0] == "'", self.records))),
            ('db.quote.find({$or: [ {val: "\'"}, {val: "\'"} ] } )', list(filter(lambda r: r[0] == "'", self.records))),
            ('db.quote.find({$and: [{val: "\'"}, {val: "\'"}] })', list(filter(lambda r: r[0] == "'", self.records))),
        ])


