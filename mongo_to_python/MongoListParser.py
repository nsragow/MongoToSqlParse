from mongo_to_python.Constants import OPEN_SQUARE, CLOSE_SQUARE, COMMA, QUOTE_SINGLE, QUOTE_DOUBLE
from mongo_to_python.FoundValue import FoundValue
from mongo_to_python.MongoToPythonType import cast_non_str_primitive


def list_parser(lis: str) -> list:
    """
    Converts mongo list into python list. Utilizes cast_non_str_primitive for the individual values.

    Example: '[false, null, 1]' -> [False, None, 1]
    :param lis: mongo list encoded as a string
    :return: python list with members converted to python values
    """
    # cleaning
    lis = lis.strip()
    assert lis[0] == OPEN_SQUARE and lis[-1] == CLOSE_SQUARE
    lis = lis[1:-1].strip()

    # used for state tracking
    opened_single_quote = False
    opened_double_quote = False

    # Is false when passing over marker characters (ex/ ',') and strippable characters
    cur_parsing = False
    # Track the start index in the string for slicing when the value terminates
    start_ind = None
    return_list = []  # The python list to return
    # iterate through all characters
    for i in range(len(lis)):
        found_val = FoundValue()  # to store a value when the parser finds one
        cur_char = lis[i]
        if not cur_parsing:  # look out for keepable chars. Otherwise keep ignoring chars
            if cur_char.strip() and cur_char != COMMA:  # found a keepable char!
                # start_parsing
                start_ind = i  # keep track where this value started
                cur_parsing = True
                # quotes mean that the value is a string and special chars should be ignored until the closing
                # of the quote
                if cur_char == QUOTE_SINGLE:
                    opened_single_quote = True
                elif cur_char == QUOTE_DOUBLE:
                    opened_double_quote = True
                elif i == len(lis) - 1:  # the last char
                    # a special case scenario where the last character of the list
                    # string is a one character value. Ex/ "3,5,6" the 6 should be immediately
                    # parsed because there is no more iteration
                    found_val.set(cast_non_str_primitive(cur_char))
        else:  # parsing
            # first check if a string value should be closed
            if opened_single_quote or opened_double_quote:
                assert not (opened_double_quote and opened_single_quote)
                quote_to_look_for = QUOTE_SINGLE if opened_single_quote else QUOTE_DOUBLE
                if cur_char == quote_to_look_for:
                    opened_single_quote = False
                    opened_double_quote = False
                    assert lis[start_ind] == quote_to_look_for
                    # dont include the first and last chars (quotes), they are to mark the string
                    found_val.set(lis[start_ind+1:i])
            # otherwise consider nonstring values
            elif cur_char == COMMA or not cur_char.strip():  # empty or comma ends a value
                found_val.set(cast_non_str_primitive(lis[start_ind:i]))  # dont include the comma or empty
            elif i == len(lis) - 1:  # looking for a non string value and this is the last iteration
                found_val.set(cast_non_str_primitive(lis[start_ind:i + 1]))  # keep the current char (its part of the value)
        if found_val:
            cur_parsing = False  # not parsing to allow passing over whitespace
            return_list.append(found_val.value)
    assert not cur_parsing  # dont leave in middle of parsing!!!
    return return_list
