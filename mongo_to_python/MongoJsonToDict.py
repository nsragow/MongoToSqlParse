# todo: remove code dup

"""
{ type:{$ne: "rose"}, type: 1, $or: [{type:{$ne: "rose"}}, {type:{$ne: "rose"}}]}
        v
{'type': {'$ne': 'rose'}, 'type': 1, '$or': [...]}

{key: leaf, plain-mapping, key: [to_dict, to_dict]

{type:{$in: ["rose", "dose"]}}

"""

from mongo_to_python.Constants import CLOSE_SQUARE, OPEN_SQUARE, QUOTE_DOUBLE, QUOTE_SINGLE, COMMA, OPEN_CURLY, \
    CLOSE_CURLY, COLON

# dict value types
from mongo_to_python.MongoJsonLeafToDict import leaf_parse
from mongo_to_python.MongoToPythonType import cast_non_str_primitive
from mongo_to_python.FoundValue import FoundValue

# possible children of a query dict key
LEAF = 1
LIST = 2
PRIMITIVE = 3


def handle_list_string(list_string: str) -> list:
    """
    Handles a list of conditions in a mongo query using the $and/$or operator.
    A list of conditions is not to be confused with a list of values like when the $in operator is used.

    The list may contain further mongo query dicts; recursion will be used.

    Example input: [{type:{$ne: "rose"}}, {type:{$ne: "rose"}}]

    :param list_string: a list of mongo query conditions encoded as a string
    :return: a list of mongo query conditions encoded as a python list
    """
    # start by cleaning
    list_string = list_string.strip()
    assert list_string[0] == OPEN_SQUARE and list_string[-1] == CLOSE_SQUARE
    list_string = list_string[1:-1].strip()

    return_list = []  # for each query dict parsed
    started_parsing = False  # skip over whitespace
    start_ind = None  # keep track of the first non whitespace char

    # state
    opened_double_quote = False
    opened_single_quote = False
    # query dicts end with a '}', but they may recursively contain '}'. Keep track of the count.
    # When you hit zero, you found the last closing bracket.
    opened_bracket_count = 0  # {}

    # iterate through the chars
    for i in range(len(list_string)):
        cur_char = list_string[i]
        if not started_parsing:  # passing over whitespace, look for the start of a dict
            # start on a curly bracket
            if cur_char == OPEN_CURLY:
                started_parsing = True
                start_ind = i
                opened_bracket_count = 1  # needs to hit zero to close
        else:  # parsing over dict
            sub_to_dict = FoundValue()  # the str that will be recursively passed to to_dict()
            if opened_single_quote or opened_double_quote:  # ignore special chars during a string
                assert not (opened_double_quote and opened_single_quote)
                # only stay open if it was open before and the cur_char is not the close
                opened_single_quote = (opened_single_quote and cur_char != QUOTE_SINGLE)
                opened_double_quote = opened_double_quote and cur_char != QUOTE_DOUBLE
            else:
                # open a string?
                opened_single_quote = cur_char == QUOTE_SINGLE
                opened_double_quote = cur_char == QUOTE_DOUBLE
                # found a brace?
                opened_bracket_count += 1 if cur_char == OPEN_CURLY else -1 if cur_char == CLOSE_CURLY else 0
                assert opened_bracket_count >= 0
                if opened_bracket_count == 0:  # found the end of the current sub_to_dict
                    sub_to_dict.set(list_string[start_ind:i+1])  # include the ending bracket
            if sub_to_dict:  # found the end of a dict, need to recursively parse it
                return_list.append(to_dict(sub_to_dict.value))
                started_parsing = False  # pass over potential whitespace
    return return_list


def to_dict(mongo_json: str) -> dict:
    """
    Convert the encoding of a mongo query/projection dict from string to Python.
    This applies recursively to either 'query' or 'projection' in 'db.collection.find(query, projection)'

    Example mongo input: {id: {$ne: 1}, name: 'Bill'}

    :param mongo_json: string encoded mongo query or mongo projection
    :return: Python dict encoded mongo query or mongo projection
    """

    return_dict = {}  # The dict to return. Currently empty (and representing all columns/no conditions)

    # only populate the return_dict if the mongo_json has non-empty chars
    if mongo_json and mongo_json.strip():
        # clean
        mongo_json = mongo_json.strip()
        assert mongo_json[0] == OPEN_CURLY and mongo_json[-1] == CLOSE_CURLY
        mongo_json = mongo_json[1:-1].strip()  # clean out the braces

        # set up parsing state
        parsing_key = True  # always start with a key
        started_parsing = False  # are you currently iterating over characters that should be kept? assume no for now
        start_ind = None  # the starting ind of a key/value to be kept for slicing
        # the key kept while the value is parsed. When the value is parsed it will be paired with this key.
        last_key = None  # Ex/ 'type', '$or'

        # todo: duplicate from the leaf parsing
        # used to track if the parser is currently passing though
        # double quotes/single quotes/square brackets during a value parse
        # while double and single cannot be opened at the same time, list can be open while the either of the other two are
        # open. This would happen if a list has strings inside
        opened_double_quote = False
        opened_single_quote = False
        opened_bracket_count = 0  # []. Needed in case the descendants have lists.

        # opened_type:
        # a terminal leaf in the tree. See MongoJsonLeafToDict.py OR
        # a primitive mapping. Ex/ {id: 1} OR
        # a conditional with recursive dicts (i.e. a list). This can be caused by $and or $or. Recurse further with to_dict()
        opened_type = None

        # iterate over chars
        for i in range(len(mongo_json)):
            cur_char = mongo_json[i]
            if parsing_key:
                if not started_parsing:  # passing over whitespace. Look out for useful chars!
                    # dont start on a comma, it may be from after the
                    # last value parse (mappings are separated by commas)
                    if cur_char.strip() and cur_char != COMMA:  # no longer parsing over whitespace!
                        started_parsing = True
                        start_ind = i
                else:  # currently looking at key chars, look for the end of the key
                    # empty space or a colon ends a key
                    if not cur_char.strip() or cur_char == COLON:  # end of key, move on to a value parse
                        started_parsing = False  # to pass over potential whitespace separating the key and value
                        last_key = mongo_json[start_ind:i]  # collect the key
                        parsing_key = False
                        # reset relevant value state
                        opened_double_quote = False
                        opened_single_quote = False
                        opened_bracket_count = 0
            else:  # parsing value
                return_val = FoundValue()  # keep a variable to capture a value when the last char is found
                if not started_parsing:  # passing over whitespace, look out for the start of the value
                    # a colon can be a separator between key and value
                    if cur_char.strip() and cur_char != COLON:  # starting the value
                        started_parsing = True
                        start_ind = i  # keep track of the start of the value
                        # this is the opening to a leaf, and the next non-string } closes it
                        opened_type = {
                            OPEN_CURLY: LEAF,
                            OPEN_SQUARE: LIST,
                        }.get(cur_char, PRIMITIVE)
                        # set the state as applies
                        opened_bracket_count = 1 if opened_type == LIST else 0
                        opened_single_quote = cur_char == QUOTE_SINGLE
                        opened_double_quote = cur_char == QUOTE_DOUBLE
                        # if we are at the end of the mongo string now
                        # the value is a single char so it cannot be a string
                        # Collect it immediately as there will be no more iterations
                        if i == len(mongo_json) - 1:
                            return_val.set(cast_non_str_primitive(mongo_json[start_ind:i + 1]))
                else:  # parsing over value chars. Look out for the end of the value
                    if opened_type == PRIMITIVE:
                        if opened_single_quote or opened_double_quote:  # look for the end of the string
                            assert not (opened_double_quote and opened_single_quote)
                            quote_char = QUOTE_DOUBLE if opened_double_quote else QUOTE_SINGLE
                            if cur_char == quote_char:  # the string ended
                                return_val.set(mongo_json[start_ind:i+1])
                                assert return_val.value[0] == quote_char and return_val.value[-1] == quote_char
                                return_val.set(return_val.value[1:-1])  # remove the string quotes
                        else:  # nonstring primitive
                            # empty ends, comma end, curly brace ends
                            if not cur_char.strip() or cur_char == COMMA or cur_char == CLOSE_CURLY:
                                # dont include the cur_char in the primitive
                                return_val.set(cast_non_str_primitive(mongo_json[start_ind:i]))
                            # is this the last iteration? Grab the primitive value
                            elif i == len(mongo_json) - 1:  # ending the dict on a primitive numeric
                                # include the last char, its part of the primitive
                                return_val.set(cast_non_str_primitive(mongo_json[start_ind:i + 1]))
                    elif opened_type == LEAF:
                        # this type will close when the ending curly bracket is found. Be careful to not consider curly
                        # brackets inside quotes
                        if opened_double_quote or opened_single_quote:  # dont count brackets. Check for string end
                            assert not (opened_double_quote and opened_single_quote)
                            # only keep the quote (string) open if it was open and cur_char is not the end
                            opened_single_quote = opened_single_quote and cur_char != QUOTE_SINGLE
                            opened_double_quote = opened_double_quote and cur_char != QUOTE_DOUBLE
                        else:  # no quotes to worry about, any curly brackets are fair game
                            # check to open strings
                            opened_double_quote = cur_char == QUOTE_DOUBLE
                            opened_single_quote = cur_char == QUOTE_SINGLE
                            if cur_char == CLOSE_CURLY:  # ending the leaf
                                leaf_string = mongo_json[start_ind:i+1]
                                assert leaf_string[0] == OPEN_CURLY
                                return_val.set(leaf_parse(leaf_string))
                    else:  # dealing with a LIST with further possible recursion
                        # goal is to close all square brackets which means the end of the list
                        if opened_single_quote or opened_double_quote:  # todo: code dup
                            assert not (opened_double_quote and opened_single_quote)
                            # only keep the quote (string) open if it was open and cur_char is not the end
                            opened_single_quote = opened_single_quote and cur_char != QUOTE_SINGLE
                            opened_double_quote = opened_double_quote and cur_char != QUOTE_DOUBLE
                        else:
                            # open a string?
                            opened_double_quote = cur_char == QUOTE_DOUBLE
                            opened_single_quote = cur_char == QUOTE_SINGLE
                            # add relevant brackets
                            opened_bracket_count += 1 if cur_char == OPEN_SQUARE else -1 if cur_char == CLOSE_SQUARE \
                                else 0
                            assert opened_bracket_count >= 0
                            if not opened_bracket_count:  # 0 brackets. the list has ended
                                list_string = mongo_json[start_ind:i+1]  # include the closing bracket
                                assert list_string[0] == OPEN_SQUARE and list_string[-1] == CLOSE_SQUARE
                                return_val.set(handle_list_string(list_string))
                if return_val:  # was a value picked up in this char iteration?
                    parsing_key = True  # switch back to key
                    started_parsing = False  # skip over potential whitespace
                    assert last_key  # must collect a key before a value
                    # Note: this will override previously mapped values. It appears that this
                    # is how mongo queries functions, so it should be kept this way.
                    return_dict[last_key] = return_val.value
        assert parsing_key, "should not end parse without finishing the last value"
        assert not started_parsing, "Cant end while in middle of parsing!"
    return return_dict
