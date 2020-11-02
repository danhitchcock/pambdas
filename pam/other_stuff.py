from functools import reduce


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


def concat(items, axis=0, join="outer", ignore_index=False):
    """
    Appends either a DataFrame or Series.

    :param other: DataFrame or Series
    :param ignore_index: Bool, If false, will create a new index
    :return: DataFrame
    """
    # append top and bottom
    if axis == 0:
        join_on = "columns"
    else:
        join_on = "index"

    # build columns
    columns = [set(getattr(item, join_on)) for item in items]
    if join == "outer":
        columns = list(reduce(lambda x, y: x.union(y), columns))
    else:
        columns = list(reduce(lambda x, y: x.intersection(y), columns))

    # Create data, with nans if there are new columns
    data_columns = []
    for col in columns:
        temp = []
        for item in items:
            if col in item.columns:
                temp += item[col].values
            else:
                temp += [nan] * len(item)
        data_columns.append(temp)

    # new index
    if ignore_index:
        index = None
    else:
        index = []
        for item in items:
            index += item.index

    return items[0].class_init(
        {k: v for k, v in zip(columns, data_columns)}, index=index
    )
