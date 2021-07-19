from mongopython_to_sql.DictToWhere import primitive_to_string


def to_sql_insert(list_2d):
    """

    :param list_2d:
    :return: (1, 'hi'), (3, 'lo')
    """
    string_builder = []
    for record in list_2d:
        row = []
        for val in record:
            row.append(primitive_to_string(val))

        string_builder.append(f'({", ".join(row)})')
    return ', '.join(string_builder)