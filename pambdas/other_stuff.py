from functools import reduce
from copy import copy


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
    """
    Checks if an object is a 2D bool key
    """
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
    Concatenates DataFrames or Series

    :param items: list, Series or DataFrames
    :param axis: int, default 0
    :param join: str, 'inner' or 'outer'
    :param ignore_index: bool, default False
    :return: DataFrame
    """
    if hasattr(items[0], "columns"):
        return concat_df(items, axis, join, ignore_index)
    else:
        return concat_ser(items, axis, join, ignore_index)


def concat_df(items, axis=0, join="outer", ignore_index=False):
    """
    Concatenates two or more dataframes

    :param other: DataFrame or Series
    :param ignore_index: Bool, If false, will create a new index
    :return: DataFrame
    """
    # append top and bottom
    if axis == 0:
        join_on = "columns"
        index_on = "index"
    else:
        join_on = "index"
        index_on = "columns"

    # build columns
    indices = [getattr(item, join_on) for item in items]
    if join == "outer":
        indices = list(reduce(lambda x, y: list_union(x, y), indices))
    else:
        indices = list(reduce(lambda x, y: list_intersection(x, y), indices))

    # Create data, with nans if there are new columns
    data = []
    for idx in indices:
        temp = []
        for item in items:
            if idx in getattr(item, join_on):
                if axis == 0:
                    temp += item[idx].values
                else:
                    temp += item.loc[idx, :].values
            else:
                if axis == 0:
                    length = item.shape[0]
                else:
                    length = item.shape[1]
                temp += [nan] * length
        data.append(temp)

    # new index. index if axis=0, columns if axis=1
    if ignore_index:
        index = None
    else:
        index = []
        for item in items:
            index += getattr(item, index_on)

    if axis == 0:
        return items[0].class_init({k: v for k, v in zip(indices, data)}, index=index)
    return items[0].class_init(data, columns=index, index=indices)


def concat_ser(items, axis=0, join="outer", ignore_index=False):
    """
    Concatenates tweo or more Series

    :param items: list, Series
    :param axis: int, default 0
    :param join: str, 'inner' or 'outer'
    :param ignore_index: bool, default False
    :return: DataFrame
    """
    # ugly, but necessary :/
    from .dataframe import DataFrame

    # axis is zero, just append series to self
    if axis == 0:
        data = []
        index = []
        for item in items:
            data += item.values
            index += item.index
        return items[0].__class__(data, index=index, name=items[0].name)

    # otherwise...
    # generate new columns or index  based on join type
    new_index = [item.index for item in items]
    if join == "outer":
        new_index = list(reduce(lambda x, y: list_union(x, y), new_index))
    else:
        new_index = list(reduce(lambda x, y: list_intersection(x, y), new_index))

    # make the data
    data = []
    for item in items:
        data.append([item[idx] if idx in item.index else nan for idx in new_index])

    # make the index
    if ignore_index:
        columns = None
    else:
        columns = [item.name for item in items]
    return DataFrame(list(zip(*data)), columns=columns, index=new_index)


def list_intersection(a, b):
    """
    Returns the intersection of two lists
    """
    return [item for item in a if item in b]


def list_union(a, b):
    """
    Returns a deduped union of two lists
    """
    c = list(copy(a))
    for item in b:
        if item not in a:
            c.append(item)
    return c
