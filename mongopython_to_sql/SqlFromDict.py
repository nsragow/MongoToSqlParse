# todo: clean up tests with iteration
# todo: move tests to a better place ;)
from mongopython_to_sql.DictToWhere import dict_to_where_conditions
from mongopython_to_sql.DictToColumns import dict_to_columns
from mongo_to_python.MongoQueryParser import MongoQuery


def to_sql(table_name: str, conditions: dict, projection: dict) -> str:
    """
    Given a table name, a mongo query (conditions) and a mongo projection return the SQL equivalent

    :param table_name: string table name
    :param conditions: mongo query arg encoded as dict
    :param projection: mongo projection arg encoded as dict
    :return: SQL query
    """
    condition_string = dict_to_where_conditions(conditions).strip()
    if condition_string:  # only add the WHERE if there are conditions
        condition_string = f' WHERE {condition_string}'

    return f'SELECT {dict_to_columns(projection)} FROM {table_name}{condition_string};'


def sql_from_mongo(query: MongoQuery) -> str:
    """
    Deconstructs MongoQuery for to_sql function and calls to_sql.

    see to_sql
    :param query:
    :return:
    """
    return to_sql(query.table, query.conditions, query.projection)