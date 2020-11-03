"""
Contains the Series class
"""
from datetime import datetime
from .indexers import LocSer, ILocSer
from .other_stuff import nan, is_bool
import functools


class Series:
    """
    view: the actual view of the data, including step
    """

    ITERABLE_1D = (list, set, tuple)

    @classmethod
    def from_data(cls, data, index, name=None, view=slice(None, None)):
        """
        Creates a Series from data and an index
        """
        self = cls()
        self.data = data  # full 1D dataset.
        self.index = tuple(index)  # index, unique to series
        self.name = name
        self.view = view  # data[view] = the values
        self.iloc = ILocSer(self)
        self.loc = LocSer(self)
        return self

    def __init__(self, data=None, index=None, name=None):
        view = None
        if isinstance(data, self.__class__):
            data = data.data
            index = data.index
            name = data.name
            view = data.view
        elif isinstance(data, (list, set, tuple)):
            data = list(data)
            view = slice(0, len(data), 1)
        elif isinstance(data, dict):
            name, data = next(iter(data.items()))
            view = slice(0, len(data), 1)

        if data and index is None:
            index = tuple(range(len(data)))
        self.data = data
        self.view = view
        self.index = tuple(index) if index else None
        self.name = name
        self.iloc = ILocSer(self)
        self.loc = LocSer(self)
        self.str = STR(self)
        self.dt = DT(self)

    def __setitem__(self, key, value):
        self.loc.__setitem__(key, value)

    def __getitem__(self, item):
        return self.loc.__getitem__(item)

    def __str__(self):
        return (
            "Series Name: "
            + str(self.name)
            + "\n"
            + "\n".join(
                "%s: %s" % (item[0], item[1])
                for item in zip(self.index, self.data[self.view])
            )
        )

    def __repr__(self):
        return str(self)

    def index_of(self, item):
        """
        Returns the integer index of values in the index.
        If it is a tuple and it's not found, raises KeyError.
        If it is another kind of iterable and not found, adds None

        :param item: slice, iterable, any hashable object

        :return: list, slice, int
        """
        names = self.index

        if isinstance(item, self.ITERABLE_1D + (self.__class__,)):

            items = []
            for i in item:
                try:
                    items.append(names.index(i))
                except ValueError:
                    if isinstance(item, tuple):
                        raise KeyError("%s not found in index." % i)
                    else:
                        items.append(None)
            return items
        elif isinstance(item, slice):
            try:
                start = None if item.start is None else names.index(item.start)
                stop = None if item.stop is None else names.index(item.stop)
            except ValueError:
                raise KeyError(
                    "At least one of the following values is not in the index: %s %s"
                    % (item.start, item.stop)
                )
            return slice(start, stop)
        else:
            try:
                return names.index(item)
            except ValueError:
                return None

    @property
    def values(self):
        return self.data[self.view]

    def __lt__(self, other):
        ser = self.copy()
        ser.data = [item < other for item in ser.data]
        return ser

    def __le__(self, other):
        ser = self.copy()
        ser.data = [item <= other for item in ser.data]
        return ser

    def __gt__(self, other):
        ser = self.copy()
        ser.data = [item > other for item in ser.data]
        return ser

    def __ge__(self, other):
        ser = self.copy()
        ser.data = [item >= other for item in ser.data]
        return ser

    def __eq__(self, other):
        if isinstance(other, (self.ITERABLE_1D, type(self))):
            if isinstance(other, type(self)):
                if other.index != self.index:
                    raise ValueError(
                        "Can only compare identically-labeled Series objects"
                    )
                else:
                    data = [item == o for item, o in zip(self, other)]
            else:
                data = [item == o for item, o in zip(self, other)]
        else:
            data = [item == other for item in self]

        ser = self.copy()
        ser.data = data
        return ser

    def __ne__(self, other):
        ser = self.copy()
        ser.data = [item != other for item in ser.data]
        return ser

    def drop(self, labels=None):
        """
        Trims the series, breaking any shared data with others

        column_index: a column to drop
        :return:
        """
        to_delete = self.view.stop
        if labels in self.index:
            to_delete = self.index.index(labels)

        self.data = (
            self.data[self.view][0:to_delete] + self.data[self.view][to_delete + 1 :]
        )
        self.index = self.index[0:to_delete] + self.index[1 + to_delete :]

        #    and adjust our indexing
        self.view = slice(0, len(self.index), 1)
        return self

    def copy(self):
        ser = self.from_data(
            self.data[self.view], self.index, self.name, slice(0, len(self.index), 1)
        )
        return ser

    def extend(self, index_name, value=None, num=1):
        self.drop()
        self.data.extend([nan] * num)
        if isinstance(index_name, self.ITERABLE_1D + (self.__class__,)):
            self.index = self.index + tuple(index_name)
        else:
            self.index = self.index + (index_name,)
        self.view = slice(self.view.start, self.view.stop + num, 1)

    def __len__(self):
        return len(self.index)

    def __next__(self):
        for i in range(len(self)):
            try:
                yield self.iloc[i]
            except IndexError:
                return

    def __iter__(self):
        for val in self.values:
            yield val

    def bound_slice(self, slc):
        """
        Converts a slice to the actual slice used to reference the
        data. Does not raise bounds error.
        """
        start = slc.start
        stop = slc.stop

        if start is None:
            start = 0
        if stop is None:
            stop = len(self)
        if start < 0:
            start = max(self.view.stop + start * self.view.step, self.view.start)
        else:
            start = min(self.view.start + start * self.view.step, self.view.stop)
        if stop < 0:
            stop = max(self.view.stop + stop * self.view.step, self.view.start)
        else:
            stop = min(self.view.start + stop * self.view.step, self.view.stop)

        return slice(start, stop, self.view.step)

    def bound_int(self, idx):
        """
        Converts an index to the actual index of the underlying
        data. Does not raise out of range errors.
        :param idx: int, desired index.values data
        :return: int, actual index of data
        """
        if idx < 0:
            idx = len(self) + idx
        idx = self.view.start + idx * self.view.step
        return idx

    def bound_iterable(self, iterable):
        """
        Converts an iterable of desired indecies to actual index numbers
        :param iterable:
        :return: list, a lit of index of the underlying data
        """
        return [self.bound_int(item) for item in iterable]

    def astype(self, type_name, copy=True):
        res = self.apply(type_name)
        if copy:
            return res
        self.iloc[:] = res.values

    def apply(self, func, *args, **kwargs):
        cp = self.copy()
        for i, val in enumerate(cp.values):
            cp.iloc[i] = func(val, *args, **kwargs)
        return cp

    def sort_values(self, ascending=True, na_position="last"):
        """
        sort_values sorts a series using the python built-in sorted function.
        :param ascending: bool, whether or not sorted values should be ascending
        :param na_position: str, 'first' or 'last'
        :return: Series, sorted
        """
        # remove nans
        indices = [i for i, x in enumerate(self.values) if x is nan]
        nan_index = [self.index[i] for i in indices]
        new_values = self.values
        new_index = list(self.index)
        for i, idx in enumerate(indices):
            del new_values[idx - i]
            del new_index[idx - i]

        reverse = not ascending
        new_values, new_index = zip(
            *sorted(zip(new_values, new_index), reverse=reverse)
        )

        if na_position == "last":
            new_values = list(new_values) + [nan] * len(nan_index)
            new_index = list(new_index) + list(nan_index)
        else:
            new_values = [nan] * len(nan_index) + list(new_values)
            new_index = list(nan_index) + list(new_index)

        return self.from_data(new_values, new_index, name=self.name)

    def unique(self):
        return list(set(self.values))

    def __add__(self, other):
        cp = self.copy()
        if isinstance(other, self.ITERABLE_1D + (self.__class__,)):
            for i, val in enumerate(other):
                cp.data[i] += val
        else:
            for i in range(len(self)):
                cp.data[i] += other
        return cp

    def __radd__(self, other):
        try:
            return self + other
        except:
            return self
        # if other is not 0:
        #     return self + other
        # else:
        #     return self

    def __mul__(self, other):
        cp = self.copy()
        if isinstance(other, self.ITERABLE_1D + (self.__class__,)):
            for i, val in enumerate(other):
                cp.data[i] *= val
        else:
            for i in range(len(self)):
                cp.data[i] *= other
        return cp

    def __truediv__(self, other):
        cp = self.copy()
        if isinstance(other, self.ITERABLE_1D + (self.__class__,)):
            for i, val in enumerate(other):
                cp.data[i] /= val
        else:
            for i in range(len(self)):
                cp.data[i] /= other
        return cp

    def __floordiv__(self, other):
        cp = self.copy()
        if isinstance(other, self.ITERABLE_1D + (self.__class__,)):
            for i, val in enumerate(other):
                cp.data[i] //= val
        else:
            for i in range(len(self)):
                cp.data[i] //= other
        return cp

    def __pow__(self, other):
        cp = self.copy()
        if isinstance(other, self.ITERABLE_1D + (self.__class__,)):
            for i, val in enumerate(other):
                cp.data[i] **= val
        else:
            for i in range(len(self)):
                cp.data[i] **= other
        return cp

    def dropna(self):
        cp = self.copy()
        cp = cp[~self.isna()]
        return cp

    def isna(self):
        """
        Returns a bool of whether or not an item is a nan
        :return: Series
        """
        cp = self.copy()
        cp.data = [item is nan for item in self.values]
        return cp

    def __invert__(self):
        cp = self.copy()
        for i in range(len(cp)):
            cp.data[i] = not cp.data[i]
        return cp

    def mean(self, dropna=True):
        if dropna:
            values = self.dropna().values
        return sum(values) / len(values)

    def sum(self, dropna=True):
        if dropna:
            values = self.dropna().values
        res = values[0]
        for item in values[1:]:
            res += item
        return res


class STR:
    def __init__(self, obj):
        self.obj = obj

    def __getattr__(self, item):
        # must return a partialed apply, which can accept args
        return functools.partial(self.obj.apply, func=getattr(str, item))


class DT:
    def __init__(self, obj):
        self.obj = obj

    def __getattr__(self, item):
        # must return a partialed apply, which can accept args
        return functools.partial(self.obj.apply, func=getattr(datetime, item))
