# todo: move all string literals to the top
# todo: further nonsense input checks
# todo: Make error catching earlier
# todo: regex support - it is possible to do with a normal equals.
# should this be implemented by erroring out when the sql version of the regex is unsupported?
# todo: organize the state into object
# todo: make tests that check that strings are properly enclosing special characters
# TODO: move tests
# todo: make command line tool
# todo: potential issues where the float values will not be precise as a encoding
"""
Main entrypoint for parser.

Input:
    MongoDB find command.
      Ex/ db.user.find({})
Output:
    SQL select query
      Ex/ SELECT * FROM user;
"""

from mongo_to_python.MongoQueryParser import parse
from mongopython_to_sql.SqlFromDict import sql_from_mongo
import sys


def mongo_to_sql(mongo: str) -> str:
    """
    Main function. Takes a mongo find query and produces the equivalent SQL

    Example:
    input: 'db.user.find({id: true})'
    output: 'SELECT * FROM user WHERE (id = TRUE);'

    :param mongo: mongo find query
    :return: SQL select query
    """
    return sql_from_mongo(parse(mongo))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        mongo_call = sys.argv[1]
        print(mongo_to_sql(mongo_call))
    else:
        print('Supply mongo find query as first arg')

