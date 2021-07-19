class FoundValue:
    """
    Used to track values that were parsed while iterating though
    a string.
    Tracks if the value was set, which is useful when the value is set to None.
    """
    def __init__(self):
        self._found = False
        self._value = None

    def __bool__(self) -> bool:
        """
        Useful for the common shorthand of 'if FoundValue'
        :return: True if the value is found
        """
        return self._found

    def set(self, val):
        """
        Set the value and mark that it was found
        :param val: The value that was found
        """
        self._found = True
        self._value = val

    @property
    def value(self):
        """
        :return: The found value
        """
        return self._value
