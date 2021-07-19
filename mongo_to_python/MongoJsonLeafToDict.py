from mongo_to_python.Constants import CLOSE_SQUARE, OPEN_SQUARE, QUOTE_DOUBLE, QUOTE_SINGLE, COMMA, OPEN_CURLY, \
    CLOSE_CURLY, COLON
from mongo_to_python.FoundValue import FoundValue
from mongo_to_python.MongoListParser import list_parser
from mongo_to_python.MongoToPythonType import cast_non_str_primitive


def leaf_parse(leaf: str) -> dict:
    """
    Parse a mongo leaf into python dict.
    Leaf is defined as the value mapping applied to a field for filtering.
    In {name: {$ne: "rose"}}, the leaf is {$ne: "rose"}. This leaf is a recursive base case.

    Examples: '{$ne: "rose"}', '{$in: [100, 1000, \'hi\', " h o "]}', '{$ne: "rose", $gt: 100}'
    :param leaf: mongo leaf encoded as str
    :return: mongo leaf encoded as python dict
    """
    # clean
    leaf = leaf.strip()
    assert leaf[0] == OPEN_CURLY and leaf[-1] == CLOSE_CURLY
    leaf = leaf[1:-1]  # remove the braces, the parse will only consider the key/val pairs
    leaf_dict = {}  # to return.
    # the parsing rules will differ based on if the current char is part of a key or value
    currently_parsing_key = True  # start parsing a key, and keep switching off between values and keys
    # Used to skip non key/val chars (like whitespace).
    # Is false when passing over marker characters (ex/ ',') and strippable characters
    started_parse = False
    start_index = 0  # to track the beginning of a key/val
    # keep track of the last key so when a val is found it can be added to leaf_dict
    last_key = None  # Ex/s '$gt', 'id'

    # used to track if the parser is currently passing though
    # double quotes/single quotes/square brackets during a value parse
    # while double and single cannot be opened at the same time, list can be open while the either of the other two are
    # open. This would happen if a list has strings inside
    opened_double_quote = False
    opened_single_quote = False
    opened_list = False

    # used for assertions.
    special_value_chars = [OPEN_SQUARE, CLOSE_SQUARE, QUOTE_SINGLE, QUOTE_DOUBLE]

    # iterate over each char in leaf
    for i in range(len(leaf)):
        cur_char = leaf[i]
        if currently_parsing_key:
            assert cur_char not in special_value_chars, f'a key cannot have {special_value_chars}'
            if started_parse:  # the current char is part of the key or should end the key
                if cur_char == COLON: # time to switch to a value
                    # first gather the key
                    last_key = leaf[start_index:i].strip()  # dont include the colon and remove trailing whitespace
                    currently_parsing_key = False  # next chars will be for the value
                    # clear out state values with regards to value parsing
                    started_parse = False  # the next chars might be whitespace
                    # reset parse state
                    opened_double_quote = False
                    opened_single_quote = False
                    opened_list = False
            else:  # should be checking if we are still parsing whitespace or we found the beginning of the key
                # might be a comma if the last value closed before the comma
                if cur_char != COMMA and cur_char.strip() != '':
                    # start parsing now! you are looking at a key
                    start_index = i  # remember where the key started
                    started_parse = True
        else:  # parsing val
            value_to_add = FoundValue()  # to hold a found value if found in this iteration
            if not started_parse:  # passing over whitespace
                if cur_char.strip() != '':  # no longer passing over whitespace
                    start_index = i  # the value starts here
                    started_parse = True
                    opened_double_quote = cur_char == QUOTE_DOUBLE
                    opened_single_quote = cur_char == QUOTE_SINGLE
                    opened_list = cur_char == OPEN_SQUARE
                    if i == len(leaf) - 1:  # starting a value and this is the last iteration, capture it immediately!
                        # it cannot be a str (i.e. its one character long, str needs quotes)
                        value_to_add.set(cast_non_str_primitive(leaf[start_index:start_index + 1]))
            else:  # passing over val chars, look out for a val-closing char
                if opened_single_quote or opened_double_quote:
                    assert not (opened_double_quote and opened_single_quote)
                    quote_char = QUOTE_SINGLE if opened_single_quote else QUOTE_DOUBLE
                    if cur_char == quote_char:  # the string is being closed
                        opened_single_quote = False
                        opened_double_quote = False
                        # if it is a list keep going to find the close of the list
                        # otherwise...
                        if not opened_list:
                            # this is the end of a string value
                            value_to_add.set(leaf[start_index:i+1])
                            assert value_to_add.value[0] == quote_char and value_to_add.value[-1] == quote_char
                            # dont include quotes, the fact that the type is str is enough
                            value_to_add.set(value_to_add.value[1:-1])
                elif opened_list:  # look for the list close
                    # there might be strings in the list. Start a string if that is the case
                    opened_double_quote = cur_char == QUOTE_DOUBLE
                    opened_single_quote = cur_char == QUOTE_SINGLE
                    if cur_char == CLOSE_SQUARE:  # close the list
                        opened_list = False
                        value_to_add.set(leaf[start_index:i+1])  # get the string list
                        value_to_add.set(list_parser(value_to_add.value))
                else:  # parsing over non str/list value
                    assert cur_char not in special_value_chars, \
                        "should not be dealing with special value " \
                        "chars unless we are in middle of parsing a special value"
                    # if a value-ending char is found or this is the last iter close the val
                    if not cur_char.strip() or cur_char == COMMA or i == len(leaf) - 1:
                        end_index = i  # dont include the comma/whitespace
                        # if the reason of ending the parse is because this is the last character, it should be included
                        if i == len(leaf) - 1:
                            end_index += 1
                        # should end the current value
                        value_to_add.set(cast_non_str_primitive(leaf[start_index:end_index]))
            if value_to_add:  # i.e. a value was ended: need to save that value and switch to key parsing
                assert last_key is not None, 'parsed a val before a key ??'
                started_parse = False  # skip over potential whitespace/comma
                currently_parsing_key = True
                # add the key value mapping to the dict
                leaf_dict[last_key] = value_to_add.value
    # make sure parsing ended properly.
    assert not started_parse, "Should not end in middle of parsing"
    assert currently_parsing_key, "Should not have just ended a key and now parsing a value"
    assert not (opened_double_quote or opened_list or opened_single_quote), 'finished parsing without closing'
    return leaf_dict
