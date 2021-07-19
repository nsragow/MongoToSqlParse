from MongoConstants import MONGO_NULL, MONGO_TRUE, MONGO_FALSE


def cast_non_str_primitive(s: str):
    """
    Used to convert a Mongo value (encoded as a string) into a Python value.

    :param s: String representing a mongo null, boolean, numeric
    :return: converted Python value (float, int, None or bool)
    """
    if s.strip() == MONGO_NULL:
        return None
    elif s.strip() == MONGO_TRUE:
        return True
    elif s.strip() == MONGO_FALSE:
        return False
    else:
        try:
            return int(s)
        except ValueError:
            return float(s)
