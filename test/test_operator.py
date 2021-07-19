import sqlite3 as sql
import unittest

from Main import mongo_to_sql
from test.List2DToInsert import to_sql_insert


class MongoOperatorTestCase(unittest.TestCase):
    @classmethod
    def populate_sql(cls):
        c = cls.conn.cursor()
        c.execute("CREATE TABLE user (id int, name text, rate real)")
        cls.records = [
            (0, 'A', 1.1),
            (1, 'B', 10.2),
            (2, 'C', 15.3),
            (3, 'D', 20.5),
            (4, 'E', 25.7),
        ]
        c.execute(f"INSERT INTO user VALUES {to_sql_insert(cls.records)}")
        cls.conn.commit()

    @classmethod
    def setUpClass(cls) -> None:
        cls.conn = sql.connect(':memory:')
        cls.populate_sql()

    def setUp(self) -> None:
        self.c = MongoOperatorTestCase.conn.cursor()

    def tearDown(self) -> None:
        MongoOperatorTestCase.conn.commit()

    @property
    def records(self):
        return MongoOperatorTestCase.records

    def query(self, mongo):
        return list(self.c.execute(mongo_to_sql(mongo)))

    def case_iter(self, cases):
        for i in range(len(cases)):
            with self.subTest(i=i):
                result = self.query(cases[i][0])
                self.assertEqual(result, cases[i][1])

    def test_all(self):
        self.case_iter([
            ('db.user.find({},{})', self.records),
            ('db.user.find({})', self.records),
            ('db.user.find()', self.records),
            ('db.user.find( {  }  , { id: 1, name: 1, rate: 1    }  )', self.records),
        ])

    def test_in(self):
        self.case_iter([
            ('db.user.find({id: {$in: []} })', []),
            ('db.user.find({id: {$in: [0, 4, 6]} })', list(filter(lambda r: r[0] in [0, 4, 6], self.records))),
            ('db.user.find({name: {$in: ["A", \'C\']} })', list(filter(lambda r: r[1] in ['A', 'C'], self.records))),
            ('db.user.find({rate: {$in: [1.1, 25.7]} })', list(filter(lambda r: r[2] in [25.7, 1.1], self.records))),
            ('db.user.find({rate: {$in: [1.1, 25.7]} }, {})', list(filter(lambda r: r[2] in [25.7, 1.1], self.records))),
            ('db.user.find({rate: {$in: [1.1, 25.7]} }, {id:1,name:1,rate:1})', list(filter(lambda r: r[2] in [25.7, 1.1], self.records))),
        ])

    def test_eq(self):
        self.case_iter([
            ('db.user.find({id: 0},{})', list(filter(lambda r: r[0] == 0, self.records)) ),
            ('db.user.find({rate: 1.1},{})', list(filter(lambda r: r[2] == 1.1, self.records))),
            ('db.user.find({name: "C"},{})', list(filter(lambda r: r[1] == 'C', self.records)))
        ])

    def test_or(self):
        self.case_iter([
            ('db.user.find({$or: [{id: 0}, {id: 3}]})', list(filter(lambda r: r[0] in [0,3], self.records))),
            ('db.user.find({$or: [{id: 0}, {$or: [ {$or: [ {name: "C"}, {rate: 10.2} ]}, {id: 3} ]  }]})',
             list(filter(lambda r: r[0] in [0,3] or r[1] == "C" or r[2] == 10.2, self.records))),
        ])

    def test_implicit_and(self):
        self.case_iter([
            ('db.user.find({$or: [{id: 0, name: "namenothere"}, {id: 3}]})',
             list(filter(lambda r: (r[0] == 0 and r[1] == "namenothere") or r[0] == 3, self.records))),
            ('db.user.find({$or: [{id: 0, name: "A"}, {id: 3}]})',
             list(filter(lambda r: (r[0] == 0 and r[1] == "A") or r[0] == 3, self.records))),
            ('db.user.find({id: 0, name: "A", rate: 1.1})',
             list(filter(lambda r: r[0] == 0 and r[1] == "A" and r[2] == 1.1, self.records))),
            ('db.user.find({id: 0, name: "A", rate: 1.01})',
             list(filter(lambda r: r[0] == 0 and r[1] == "A" and r[2] == 1.01, self.records))),
        ])

    def test_explicit_and(self):
        self.case_iter([
            ('db.user.find({$or: [ {$and: [{id: 0}, {name: "namenothere"}]}, {id: 3}]})',
             list(filter(lambda r: (r[0] == 0 and r[1] == "namenothere") or r[0] == 3, self.records))),
            ('db.user.find({$or: [{$and: [{id: 0}, {name: "A"}]}, {id: 3}]})',
             list(filter(lambda r: (r[0] == 0 and r[1] == "A") or r[0] == 3, self.records))),
            ('db.user.find({$and: [{id: 0}, {name: "A"}, {rate: 1.1}]})',
             list(filter(lambda r: r[0] == 0 and r[1] == "A" and r[2] == 1.1, self.records))),
            ('db.user.find({$and: [{id: 0}, {name: "A"}, {rate: 1.01}]})',
             list(filter(lambda r: r[0] == 0 and r[1] == "A" and r[2] == 1.01, self.records))),
        ])

    def test_less_than(self):
        # not going to consider string less than because I think that has different implementations
        # $lt
        self.case_iter([
            ('db.user.find({id: {$lt: -1} })', []),
            ('db.user.find({id: {$lt: 3} })', list(filter(lambda r: r[0] < 3, self.records))),
            ('db.user.find({rate: {$lt: 16.7} })', list(filter(lambda r: r[2] < 16.7, self.records))),
            ('db.user.find({rate: {$lt: 16.7} }, {})', list(filter(lambda r: r[2] < 16.7, self.records))),
            ('db.user.find({rate: {$lt: 16.7} }, {id:1,name:1,rate:1})', list(filter(lambda r: r[2] < 16.7, self.records))),
        ])

    def test_less_than_eq(self):
        self.assertNotEqual(list(filter(lambda r: r[0] <= 3, self.records)), list(filter(lambda r: r[0] < 3, self.records)))
        self.assertEqual(list(filter(lambda r: r[0] <= 3, self.records)), list(filter(lambda r: r[0] <= 3, self.records)))
        self.assertNotEqual(list(filter(lambda r: r[2] < 15.3, self.records)), list(filter(lambda r: r[2] <= 15.3, self.records)))
        self.assertEqual(list(filter(lambda r: r[2] < 15.3, self.records)), list(filter(lambda r: r[2] < 15.3, self.records)))
        self.case_iter([
            ('db.user.find({id: {$lte: -1} })', []),
            ('db.user.find({id: {$lt: 3} })', list(filter(lambda r: r[0] < 3, self.records))),
            ('db.user.find({id: {$lte: 3} })', list(filter(lambda r: r[0] <= 3, self.records))),
            ('db.user.find({rate: {$lte: 15.3} })', list(filter(lambda r: r[2] <= 15.3, self.records))),
            ('db.user.find({rate: {$lt: 15.3} })', list(filter(lambda r: r[2] < 15.3, self.records))),
            ('db.user.find({rate: {$lte: 15.3} }, {})', list(filter(lambda r: r[2] <= 15.3, self.records))),
            ('db.user.find({rate: {$lte: 15.3} }, {id:1,name:1,rate:1})', list(filter(lambda r: r[2] <= 15.3, self.records))),
        ])

    def test_greater_than(self):
        self.case_iter([
            ('db.user.find({id: {$gt: -1} })', self.records),
            ('db.user.find({id: {$gt: 3} })', list(filter(lambda r: r[0] > 3, self.records))),
            ('db.user.find({rate: {$gt: 16.7} })', list(filter(lambda r: r[2] > 16.7, self.records))),
            ('db.user.find({rate: {$gt: 16.7} }, {})', list(filter(lambda r: r[2] > 16.7, self.records))),
            ('db.user.find({rate: {$gt: 16.7} }, {id:1,name:1,rate:1})', list(filter(lambda r: r[2] > 16.7, self.records))),
        ])

    def test_greater_than_eq(self):
        self.assertEqual(list(filter(lambda r: r[0] > 3, self.records)), list(filter(lambda r: r[0] > 3, self.records)))
        self.assertNotEqual(list(filter(lambda r: r[0] > 3, self.records)), list(filter(lambda r: r[0] >= 3, self.records)))
        self.assertEqual(list(filter(lambda r: r[2] >= 15.3, self.records)),list(filter(lambda r: r[2] >= 15.3, self.records)))
        self.assertNotEqual(list(filter(lambda r: r[2] >= 15.3, self.records)),list(filter(lambda r: r[2] > 15.3, self.records)))
        self.case_iter([
            ('db.user.find({id: {$gte: -1} })', self.records),
            ('db.user.find({id: {$gt: 3} })', list(filter(lambda r: r[0] > 3, self.records))),
            ('db.user.find({id: {$gte: 3} })', list(filter(lambda r: r[0] >= 3, self.records))),
            ('db.user.find({rate: {$gte: 15.3} })', list(filter(lambda r: r[2] >= 15.3, self.records))),
            ('db.user.find({rate: {$gte: 15.3} }, {})', list(filter(lambda r: r[2] >= 15.3, self.records))),
            ('db.user.find({rate: {$gte: 15.3} }, {id:1,name:1,rate:1})', list(filter(lambda r: r[2] >= 15.3, self.records))),
        ])

    def test_not_equal(self):
        self.assertEqual(list(filter(lambda r: r[0] != -1, self.records)), self.records)
        self.assertNotEqual(list(filter(lambda r: r[2] != 15.3, self.records)), self.records)
        self.assertNotEqual(list(filter(lambda r: r[1] != "A", self.records)), self.records)
        self.case_iter([
            ('db.user.find({id: {$ne: -1} })', self.records),
            ('db.user.find({id: {$ne: 3} })', list(filter(lambda r: r[0] != 3, self.records))),
            ('db.user.find({name: {$ne: 3} })', list(filter(lambda r: r[1] != 3, self.records))),
            ('db.user.find({name: {$ne: \'A\'} })', list(filter(lambda r: r[1] != 'A', self.records))),
            ('db.user.find({rate: {$ne: 15.3} })', list(filter(lambda r: r[2] != 15.3, self.records))),
        ])

    def test_projection(self):
        self.case_iter([
            ('db.user.find({}, {id: 1})', list(map(lambda r: tuple([r[0]]), self.records))),
            ('db.user.find({}, {id: true})', list(map(lambda r: tuple([r[0]]), self.records))),
            ('db.user.find({}, {id: true, name: 1})', list(map(lambda r: tuple([r[0], r[1]]), self.records))),
            ('db.user.find({}, {id: 1, rate: true})', list(map(lambda r: tuple([r[0], r[2]]), self.records))),
            # not sure if order changes are supposed to be supported but here it is
            ('db.user.find({}, {rate: 1, id: true})', list(map(lambda r: tuple([r[2], r[0]]), self.records))),
            ('db.user.find({}, {id: 1, name:1 ,rate: true})', self.records),
        ])

