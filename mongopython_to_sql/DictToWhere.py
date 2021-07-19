# Mongo query language definition:
#
# key:
#  - column name
#  - and/or
#  - in cases where the parent is a column name, can be column name operators
#
#  {_id: {$gt: 100, $lt:200}}
#  is the same as
#  $and: [{_id: {$gt: 100}},{_id: {$lt: 200}}]

# Column name operators
from MongoConstants import MONGO_NOT_EQUAL, MONGO_IN, MONGO_OR, MONGO_AND
from SqlConstants import OPERATOR_MAPPING, SQL_AND, SQL_OR, SQL_IS, SQL_IS_NOT, SQL_EQ, SQL_IN, SQL_NULL

# Operators that can appear as keys instead of column names
TOP_LEVEL_OPERATORS = [MONGO_OR, MONGO_AND]


def dict_to_where_conditions(dict_where: dict) -> str:
    """
    Convert a mongo query argument encoded as a Python dict to
    as SQL condition list

    Ex/ input: '{id: 1}' output: 'id = 1'

    Note: This function does not append the WHERE operator.
    :param dict_where: mongo query argument encoded as a Python dict
    :return: sql condition list
    """
    where_conditions = []  # to be returned
    if dict_where:
        for key, val in dict_where.items():
            sql_condition = None
            if key not in TOP_LEVEL_OPERATORS:  # This is a column_name condition (Ex/ {id: 1})
                sql_condition = column_name_condition(key, val)
            else:  # this is a condition list (Ex/ {$or: [{...}, {...}]}
                assert key in TOP_LEVEL_OPERATORS, "If its not a column name, then it must be a top level operator"
                sql_condition = and_or_condition(key, val)
            assert sql_condition is not None  # todo: am i covering implicit and?
            where_conditions.append(sql_condition)
        return f' {SQL_AND} '.join(where_conditions)  # multiple mongo conditions are combined with AND
    else:
        # the dict is None or empty. There should be no WHERE condition
        return ''


def column_name_condition(column_name: str, condition) -> str:
    """
    Converts column_name and condition parsed from a mongo query into SQL condition.

    A column name condition will have a column name and condition that is
    either a primitive value (in the case of equals) or a dict with column_name conditions ($gt, $lt, etc)

    This is a base case as TOP_LEVEL_OPERATORS cannot be in the condition.

    Example:
      input: {_id: {$gt: 100, $lt:200}}
      column_name='_id' condition='{$gt: 100, $lt:200}'
      output: '(_id > 100 AND _id < 200)'
    :param column_name:
    :param condition: Either primitive value for an -eq operator or a dictionary of column_name conditions
    :return: SQL conditions
    """
    assert is_primitive(condition) or type(condition) == dict, "Must have some sort of valid conditions for the column"
    assert column_name, "Cannot have an empty column name"
    # allowed dict keys: column_name operators
    condition_list = []  # to return
    if is_primitive(condition):  # ex/ {id: 1}
        # SQL requires IS when comparing NULL, otherwise = will do
        comparator_string = SQL_EQ if condition is not None else SQL_IS
        # format SQL
        condition_list.append(f'{column_name} {comparator_string} {primitive_to_string(condition)}')
    else:  # operator condition Ex/ {id: {$ne: 1}}
        assert type(condition) == dict
        # There can be multiple operator conditions
        # Ex/ {id: {$ne: 1, $lt: 4}}
        for column_name_condition_type, primitive in condition.items():
            if column_name_condition_type == MONGO_IN:  # {id: {$in: [...]}}
                assert type(primitive) == list
                # All values in a list must be primitives
                _list = [primitive_to_string(x) for x in primitive]
                # format SQL
                condition_list.append(f'{column_name} {SQL_IN} ({", ".join(_list)})')  # Ex/ 'key IN (1, '4', 2)
                # todo: SQL may not support type mixing in lists as seen above
            else:  # all other conditions will have a direct primitive instead of list
                assert column_name_condition_type in OPERATOR_MAPPING
                sql_operator = OPERATOR_MAPPING[column_name_condition_type]  # get SQL operator
                # special case when the primitive is null and the operator is $ne because
                # sql does not recognize where col != null
                if primitive is None and column_name_condition_type == MONGO_NOT_EQUAL:
                    sql_operator = SQL_IS_NOT
                condition_list.append(f'{column_name} {sql_operator} {primitive_to_string(primitive)}')  # format sql
    return f'({f" {SQL_AND} ".join(condition_list)})'  # {id: {$ne: 1, $lt: 4}} in SQL id != 1 AND id < 4


def and_or_condition(and_or: str, condition_list: list) -> str:
    """
    Create a list of conditions joined by SQL AND/OR. Use parenthesis to keep order of operations.

    :param and_or: mongo TOP_LEVEL_OPERATORS. Can be $and or $or.
    :param condition_list: List of top_level conditions to be joined
    :return: compound SQL conditions
    """
    assert and_or in TOP_LEVEL_OPERATORS
    and_or_string = f" {SQL_AND} " if and_or == MONGO_AND else f" {SQL_OR} "
    return f'({and_or_string.join([dict_to_where_conditions(x) for x in condition_list])})'


def primitive_to_string(primitive):
    """
    Take python primitive (term used loosely) and convert them to an SQL string.

    :param primitive: Python primitive
    :return: SQL string
    """
    # in the case that the value was a string, add quotes (SQL requires quotes around a string)
    assert is_primitive(primitive)
    if type(primitive) == str:
        primitive = primitive.replace("'", "''")  # escape the single quotes by doubling them up
        return f"'{primitive}'"
    elif primitive is None:
        return SQL_NULL
    elif type(primitive) == bool:
        return 'TRUE' if primitive else 'FALSE'
    else:  # otherwise Python string casting works (for numeric)
        return f'{primitive}'


def is_primitive(to_check) -> bool:
    """
    Check if value is primitive for the scope of this project.

    :param to_check: value to check
    :return: True if value is primitive else False
    """
    return type(to_check) in [str, int, float, bool] or to_check is None
