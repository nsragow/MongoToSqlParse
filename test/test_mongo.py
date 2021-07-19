import unittest

from mongo_to_python.MongoJsonLeafToDict import leaf_parse
from mongo_to_python.MongoJsonToDict import to_dict


class MyTestCase(unittest.TestCase):
    def test_leaf(self):
        self.assertEqual(leaf_parse('{$ne: "rose"}'), {'$ne': 'rose'})
        self.assertEqual(leaf_parse(' {  $ne   :    "rose"   }   '), {'$ne': 'rose'})
        self.assertEqual(leaf_parse(' {  $ne   :"rose"   }   '), {'$ne': 'rose'})
        self.assertEqual(leaf_parse('{$ne: 100}'), {'$ne': 100})
        self.assertEqual(leaf_parse('{$ne: 100.5}'), {'$ne': 100.5})
        self.assertEqual(leaf_parse('{ $ne  :100  }'), {'$ne': 100})
        self.assertEqual(leaf_parse('{$ne: \'100\'}'), {'$ne': '100'})
        self.assertEqual(leaf_parse('{$ne :\'100\'}'), {'$ne': '100'})
        self.assertEqual(leaf_parse('{id: \'100\'}'), {'id': '100'})
        self.assertEqual(leaf_parse('{$in: [100, 1000, \'hi\', " h o "]}'), {'$in': [100, 1000, 'hi', ' h o ']})
        self.assertEqual(leaf_parse('{ $in  :[100  ,1000  , \'hi\' ," h o "]}'), {'$in': [100, 1000, 'hi', ' h o ']})
        self.assertEqual(leaf_parse('{\n $in\n  :\n[100  \n,1000 \t , \n\'hi\' ," h o "]}\n'), {'$in': [100, 1000, 'hi', ' h o ']})
        self.assertEqual(leaf_parse('{$ne: "rose", $gt: 100}'), {'$ne': 'rose', '$gt': 100})

    def test_to_dict(self):
        val = to_dict('{ type:{$ne: "rose"}, type: 1, $or: [{type:{$ne: "rose"}}, {type:{$ne: "rose"}}]}')
        val2 = to_dict('{ type: 1, type:{$ne: "rose"},  $or: [{type:{$ne: "rose"}}, {type:{$ne: "rose"}}]}')
        self.assertEqual(val, {'type': 1, '$or': [{'type':{'$ne': "rose"}}, {'type':{'$ne': "rose"}}]})
        self.assertEqual(val2, {'type': {'$ne': 'rose'}, '$or': [{'type':{'$ne': "rose"}}, {'type':{'$ne': "rose"}}]})
        self.assertEqual(to_dict('{_id: 1}'), {'_id': 1})


if __name__ == '__main__':
    unittest.main()
