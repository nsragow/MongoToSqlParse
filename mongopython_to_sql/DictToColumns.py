# todo: while exclusion projections should not be supported due to the lack of a column list,
#  there may be other projections that can be supported
from SqlConstants import SQL_ALL

UNSUPPORTED_ERROR = 'Only supports projections with value of 1 of True. ' \
                    'Sorry I don\'t know all the columns to work by exclusion'


def dict_to_columns(dict_projection: dict) -> str:
    """
    Convert a mongo projection argument encoded as a Python dict to
    as SQL column list

    Note: Only supports projections with a value of 1 or True. Any
    other projection value will throw an AssertionError

    :param dict_projection: mongo projection encoded as Python dict
    :return: str SQL column list (Ex/ *)
    """
    if dict_projection:
        columns = []
        # Add each key as a column assuming their value is 1 or True
        for key in dict_projection:
            assert dict_projection[key] in (1, True), UNSUPPORTED_ERROR
            columns.append(key)
        return ', '.join(columns)  # Ex/ 'id, name, age'
    else:
        # the dict is None or empty. The SQL columns should then be *
        return SQL_ALL
