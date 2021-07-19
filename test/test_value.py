import unittest
import sqlite3 as sql

from Main import mongo_to_sql
from test.List2DToInsert import to_sql_insert


class MongoValueTestCase(unittest.TestCase):
    @classmethod
    def populate_sql(cls):
        c = cls.conn.cursor()
        c.execute("CREATE TABLE manager (id int, name text, rate real, active boolean)")
        cls.records = [
            (0, 'A', None, True),
            (1, None, 10.2, False),
            (2, 'C', None, True),
            (3, None, 20.5, False),
            (4, 'E', 25.7, None),
        ]
        c.execute(f"INSERT INTO manager VALUES {to_sql_insert(cls.records)}")
        cls.conn.commit()

    @classmethod
    def setUpClass(cls) -> None:
        cls.conn = sql.connect(':memory:')
        cls.populate_sql()

    def setUp(self) -> None:
        self.c = MongoValueTestCase.conn.cursor()

    def tearDown(self) -> None:
        MongoValueTestCase.conn.commit()

    @property
    def records(self):
        return MongoValueTestCase.records

    def query(self, mongo):
        sql_string = mongo_to_sql(mongo)
        return list(self.c.execute(sql_string))

    def case_iter(self, cases):
        for i in range(len(cases)):
            with self.subTest(i=i):
                result = self.query(cases[i][0])
                if len(cases[i]) == 3:
                    self.assertEqual(result, cases[i][1], cases[i][2])
                else:
                    self.assertEqual(result, cases[i][1])


    def test_all(self):
        self.case_iter([
            # ('db.manager.find({})', self.records),
            ('db.manager.find({id: {$ne: null}})', self.records),

        ])

    def test_null(self):
        # todo: there was an issue where when a parse found a None value, it set the
        # found value to None which did not trigger the found values (as it checks for None as a way
        # to see if it was found)
        self.case_iter([
            ('db.manager.find({id: null})', []),
            ('db.manager.find({name: null})', list(filter(lambda r: r[1] is None, self.records))),
            ('db.manager.find({rate: null})', list(filter(lambda r: r[2] is None, self.records))),
            ('db.manager.find({active: null})', list(filter(lambda r: r[3] is None, self.records))),
            ('db.manager.find({id: {$ne: null}})', list(filter(lambda r: r[0] is not None, self.records))),
            ('db.manager.find({name: {$ne: null}})', list(filter(lambda r: r[1] is not None, self.records))),
            ('db.manager.find({rate: {$ne: null }})', list(filter(lambda r: r[2] is not None, self.records))),
            ('db.manager.find({active: {$ne: null}})', list(filter(lambda r: r[3] is not None, self.records))),
            ('db.manager.find({id: {$in: [null]}})', list(filter(lambda r: r[0] in [None], self.records))),
            ('db.manager.find({id: {$in: [null, null   ]}})', list(filter(lambda r: r[0] in [None], self.records))),
        ])

    def test_bool(self):
        self.case_iter([
            # SQL nulls complicates this test a bit.
            # in python None != False but SQL will not return the row
            ('db.manager.find({active: {$ne: true}})', list(filter(lambda r: r[3] is not None and r[3] != True, self.records)), '$ne true'),
            ('db.manager.find({active: true})', list(filter(lambda r: r[3] == True, self.records)), '= true'),
            ('db.manager.find({active: {$ne: false}})', list(filter(lambda r: r[3] is not None and r[3] != False, self.records)), '$ne true'),
            ('db.manager.find({active: false})', list(filter(lambda r: r[3] == False, self.records))),
            ('db.manager.find({active: {$in: [true, false]}})', list(filter(lambda r: r[3] == False or r[3] == True, self.records))),  # fails becuase eval does not recognize true and false
            ('db.manager.find({active: {$in: [true]}})', list(filter(lambda r: r[3] == True, self.records))),  # fails becuase eval does not recognize true and false
            ('db.manager.find({active: {$in: [false]}})', list(filter(lambda r: r[3] == False, self.records))),  # fails becuase eval does not recognize true and false
        ])
