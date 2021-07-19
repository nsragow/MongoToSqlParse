# todo: messy
from mongo_to_python.MongoJsonToDict import to_dict
from mongo_to_python.Constants import CLOSE_SQUARE, OPEN_SQUARE, QUOTE_DOUBLE, QUOTE_SINGLE, COMMA, OPEN_CURLY, \
    CLOSE_CURLY, COLON, OPEN_PAREN, CLOSE_PAREN

DB = 'db'
FIND = 'find'


class MongoQuery:
    """
    Container for a mongo find query.
    Has the table to query, query arg (conditions) and projection.
    """
    def __init__(self, table: str, conditions: dict, projection: dict):
        self.table = table
        self.conditions = conditions
        self.projection = projection

    def __str__(self):
        return f'MongoQuery: table={self.table} conditions={str(self.conditions)} projection={str(self.projection)}'


def get_json_end_index(str_with_json) -> int:
    """
    Get the ending index of the first mongo-json dict
    :param str_with_json:
    :return:
    """
    # state
    open_brackets = 0
    open_double_quote = False
    open_single_quote = False
    # iterate chars
    for i in range(len(str_with_json)):
        cur_char = str_with_json[i]
        if open_single_quote or open_double_quote:  # skip normal counting in case of string
            assert not (open_single_quote and open_double_quote)
            # string should stay open if it was already and the close is not found
            open_double_quote = open_double_quote and cur_char != QUOTE_DOUBLE
            open_single_quote = open_single_quote and cur_char != QUOTE_SINGLE
        else:
            # Open a string?
            open_single_quote = cur_char == QUOTE_SINGLE
            open_double_quote = cur_char == QUOTE_DOUBLE
            # count braces
            open_brackets += 1 if cur_char == OPEN_CURLY else -1 if cur_char == CLOSE_CURLY else 0
            assert open_brackets >= 0
            if open_brackets == 0:
                return i
    raise ValueError('Malformed opening/closing braces')


def parse(mongo_call: str) -> MongoQuery:
    """
    Split up a string encoded mongo find call into a MongoQuery.

    Example input: 'db.user.find({})'
    :param mongo_call: String encoded mongo find call
    :return: MongoQuery containing the db name, conditions and projection
    """
    # First split off the db_string, table_name and find string
    db_string, mongo_call = mongo_call.split('.', 1)
    assert db_string == DB  # todo: in mongodb, does this always have to be 'db'? Not sure
    table_name, mongo_call = mongo_call.split('.', 1)
    mongo_call = mongo_call.strip()
    assert mongo_call.startswith(FIND)
    mongo_call = mongo_call.replace(FIND, '', 1).strip()  # dont need to keep the find, just making sure its there
    assert mongo_call[0] == OPEN_PAREN and mongo_call[-1] == CLOSE_PAREN  # arguments must be contained by parenthesis
    mongo_call = mongo_call[1:-1].strip()  # remove the parenthesis

    # now split up the query/projection args
    query_json = None
    projection_json = None
    if OPEN_CURLY in mongo_call:  # i.e. args always have braces
        assert mongo_call[0] == OPEN_CURLY
        index_of_json_end = get_json_end_index(mongo_call)
        query_json = mongo_call[:index_of_json_end+1]  # get the query_json
        mongo_call = mongo_call[index_of_json_end+1:].strip()  # and remove it from the mongo_call string
        # if there is a projection arg, there must be a comma separating it
        if mongo_call and mongo_call[0] == COMMA:
            # since there is another arg and there can only be two, the rest of the string is the projection arg
            projection_json = mongo_call[1:].strip()
    return MongoQuery(table_name,
                      to_dict(query_json),
                      to_dict(projection_json))
