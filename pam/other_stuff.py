class NaN:
    def __eq__(self, *args):
        return False

    def __lt__(self, *args):
        return False

    def __le__(self, *args):
        return False

    def __gt__(self, *args):
        return False

    def __ge__(self, *args):
        return False

    def __str__(self):
        return "NaN"

    def __repr__(self):
        return self.__str__()

    def __add__(self, other):
        return self

    def __radd__(self, other):
        return self

    def __truediv__(self, other):
        return self

    def __floordiv__(self, other):
        return self

    def __mul__(self, other):
        return self

    def __pow__(self, other):
        return self

    def __mod__(self, other):
        return self


nan = NaN()


def is_bool(key):
    """
    Checks if the first value in some kind of item is a boolean value
    :param key:
    :return:
    """
    try:
        item0 = key.iloc[0]
    except AttributeError:
        if isinstance(key, (list, tuple, set)):
            item0 = key[0]
        else:
            item0 = key
    if isinstance(item0, bool):
        return True
    return False


def is_2d_bool(key):
    try:
        item0 = key.iloc[0, 0]
    except AttributeError:
        try:
            item0 = key[0][0]
        except:
            return False
    if isinstance(item0, bool):
        return True
    return False


def invert(item):
    """
    Copies and inverts a list or nested list
    :param item: a nd iterable of boolean values
    :return: ~item
    """
    res = []
    if hasattr(item[0], "__len__"):
        for i in range(len(item)):
            res.append(invert(item[i]))
    else:
        res = [not val for val in item]
        return res
    return res
