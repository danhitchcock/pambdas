from copy import copy
import itertools
import gc


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


nan = NaN()


class Series:
    """
    view: the actual view of the data, including step
    """

    ITERABLE_1D = (list, set, tuple)

    @classmethod
    def from_data(cls, data, index, name=None, view=slice(None, None)):
        self = cls()
        self.data = data  # full 1D dataset.
        self.index = index  # index, unique to series
        self.name = name
        self.view = view  # data[view] = the values
        self.iloc = ILoc(self)
        self.loc = Loc(self)
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
        self.iloc = ILoc(self)

    def __setitem__(self, key, value):
        self.loc.__setitem__(key, value)

    def __getitem__(self, item):
        self.loc.__getitem__(item)

    def __str__(self):

        return (
            "Series Name: "
            + str(self.name)
            + "\n"
            + "\n".join(
                "%s: %s" % (item[0], item[1])
                for item in zip(self.index, self.data[self.view],)
            )
        )

    def __repr__(self):
        return str(self)

    def __iter__(self):
        return iter(self.data[self.view])

    def index_of(self, item, axis=None):
        names = self.index

        if isinstance(item, self.ITERABLE_1D + (self.__class__,)):
            # bypass for boolean
            if is_bool(item):
                return item
            return [names.index(i) for i in item]
        elif isinstance(item, slice):
            start = None if item.start is None else names.index(item.start)
            stop = None if item.stop is None else names.index(item.stop)
            return slice(start, stop)
        else:
            try:
                return names.index(item)
            except:
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
            self.data[self.view], self.index, self.name, slice(0, len(self.index))
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


class ILoc:
    ITERABLE_1D = (list, set, tuple, Series)

    def __init__(self, obj):
        self.obj = obj

    def __getitem__(self, items):
        # tacky, but whatever
        if isinstance(self.obj, DataFrame):
            return self.getitem_df(items)
        elif isinstance(self.obj, Series):
            return self.getitem_ser(items)

    def __setitem__(self, items, value):
        # tacky, but whatever
        if isinstance(self.obj, DataFrame):
            return self.setitem_df(items, value)
        elif isinstance(self.obj, Series):
            return self.setitem_ser(items, value)

    def setitem_df(self, items, value):
        data = self.obj.data
        step = self.obj.step

        # if it's a tuple, its multiple indicies. Otherwise, make a dummy index
        if isinstance(items, tuple):
            items = list(items)
        else:
            items = [items, slice(None, None)]
        data_items = copy(items)

        # convert to bool, or bound
        for i, item in enumerate(items):
            if isinstance(item, self.ITERABLE_1D):
                # if it's a boolean
                if is_bool(item):
                    items[i] = [i for i, val in enumerate(item) if val]
                data_items[i] = self.obj.bound_iterable_to_df(items[i], axis=i)
            elif isinstance(item, slice):
                items[i] = self.obj.convert_slice(item, axis=i)
                data_items[i] = self.obj.bound_slice_to_df(items[i], axis=i)
            elif isinstance(item, int):
                data_items[i] = self.obj.bound_int_to_df(item, axis=i)
        del items

        #################
        # Returns an item
        #################
        if isinstance(data_items[0], int) and isinstance(data_items[1], int):
            # eg [1, 0]
            data[data_items[0] + step * data_items[1]] = value
        ##################
        # Returns a Series
        ##################
        if isinstance(data_items[0], slice) and isinstance(data_items[1], int):
            # eg [1:3, 0]
            if not isinstance(value, self.ITERABLE_1D):
                value = [value] * (data_items[0].stop - data_items[0].start)

            data[
                data_items[0].start
                + step * data_items[1] : data_items[0].stop
                + data_items[1] * step
            ] = value

        if isinstance(data_items[0], int) and isinstance(data_items[1], slice):
            # eg .iloc[0, 1:3]
            start = data_items[0] + step * data_items[1].start
            stop = data_items[0] + step * data_items[1].stop
            if not isinstance(value, self.ITERABLE_1D):
                value = [value] * (data_items[1].stop - data_items[1].start)

            for i, val in zip(range(start, stop, step), value):
                data[i] = val
        if isinstance(data_items[0], int) and isinstance(
            data_items[1], self.ITERABLE_1D
        ):
            # eg .iloc[0, [1, 2, 3]]
            if not isinstance(value, self.ITERABLE_1D):
                value = [value] * (len(data_items[1]))
            for i, val in zip(items[1], value):
                data[data_items[0] + step * i] = val

        if isinstance(data_items[0], self.ITERABLE_1D) and isinstance(
            data_items[1], int
        ):
            # eg .iloc[[1, 2, 3], 0]
            if not isinstance(value, self.ITERABLE_1D):
                value = [value] * (len(data_items[0]))
            for i, val in zip(data_items[0], value):
                data[i + step * data_items[1]] = val

        #####################
        # Returns a DataFrame
        #####################
        # warning: everything below is very messy.

        if isinstance(data_items[0], slice) and isinstance(data_items[1], slice):
            # e.g. .iloc[1:3, :]
            if not isinstance(value, self.ITERABLE_1D):
                pass
            # there is almost certainly a better way to do this
            k = 0
            if isinstance(value, self.ITERABLE_1D):
                value = list(itertools.chain.from_iterable(value))
            else:
                value = [value] * (
                    (data_items[1].stop - data_items[1].start)
                    * (data_items[0].stop - data_items[0].start)
                )

            for i in range(data_items[0].start, data_items[0].stop):
                for j in range(data_items[1].start, data_items[1].stop):
                    data[i + j * step] = value[k]
                    k += 1
        if isinstance(data_items[0], self.ITERABLE_1D) and isinstance(
            data_items[1], slice
        ):
            # e.g. .iloc[[1, 2], :]
            # there is almost certainly a better way to do this
            k = 0
            if isinstance(value, self.ITERABLE_1D):
                value = list(itertools.chain.from_iterable(value))
            else:
                value = [value] * (
                    (data_items[1].stop - data_items[1].start) * len(data_items[0])
                )

            for i in data_items[0]:
                for j in range(data_items[1].start, data_items[1].stop):
                    data[i + j * step] = value[k]
                    k += 1
        if isinstance(data_items[0], slice) and isinstance(
            data_items[1], self.ITERABLE_1D
        ):
            # e.g. .iloc[:, [1,2]
            k = 0
            if isinstance(value, self.ITERABLE_1D):
                value = list(itertools.chain.from_iterable(value))
            else:
                value = [value] * (
                    (data_items[0].stop - data_items[0].start) * len(data_items[1])
                )

            for i in range(data_items[0].start, data_items[0].stop):
                for j in data_items[1]:
                    data[i + j * step] = value[k]
                    k += 1
        if isinstance(data_items[0], self.ITERABLE_1D) and isinstance(
            data_items[1], self.ITERABLE_1D
        ):
            # e.g. .iloc[:, [1,2]
            k = 0
            if isinstance(value, self.ITERABLE_1D):
                value = list(itertools.chain.from_iterable(value))
            else:
                value = [value] * (len(data_items[0]) * len(data_items[1]))
            for i in data_items[0]:
                for j in data_items[1]:
                    data[i + j * step] = value[k]
                    k += 1

    def getitem_df(self, items):
        data = self.obj.data
        index = self.obj.index
        columns = self.obj.columns
        step = self.obj.step
        view = self.obj.view
        print("The series: ", items)
        # if it's a tuple, its multiple indicies. Otherwise, make a dummy index
        if isinstance(items, tuple):
            items = list(items)
        else:
            items = [items, slice(None, None)]
        data_items = copy(items)

        # convert to bool, or bound
        for i, item in enumerate(items):
            if isinstance(item, self.ITERABLE_1D):
                # if it's a boolean
                if is_bool(item):
                    items[i] = [i for i, val in enumerate(item) if val]
                data_items[i] = self.obj.bound_iterable_to_df(items[i], axis=i)
            elif isinstance(item, slice):
                items[i] = self.obj.convert_slice(item, axis=i)
                data_items[i] = self.obj.bound_slice_to_df(items[i], axis=i)
            elif isinstance(item, int):
                data_items[i] = self.obj.bound_int_to_df(item, axis=i)
        #################
        # Returns an item
        #################
        if isinstance(items[0], int) and isinstance(items[1], int):
            # eg [1, 0]
            return data[data_items[0] + step * data_items[1]]
        ##################
        # Returns a Series
        ##################
        if isinstance(items[0], slice) and isinstance(items[1], int):
            # eg [1:3, 0]
            index = index[items[0]]
            name = columns[items[1]]
            data = data
            view = slice(
                data_items[0].start + data_items[1] * step,
                data_items[0].stop + data_items[1] * step,
            )
        elif isinstance(items[0], int) and isinstance(items[1], slice):
            # eg .iloc[0, 1:3]
            name = index[items[0]]
            index = columns[items[1]]
            start = data_items[0] + step * data_items[1].start
            stop = data_items[0] + step * data_items[1].stop
            view = slice(start, stop, step)
            data = data
        elif isinstance(items[0], int) and isinstance(items[1], self.ITERABLE_1D):
            # eg .iloc[0, [1, 2, 3]]
            name = index[items[0]]
            index = tuple(columns[i] for i in items[1])
            data = [data[data_items[0] + step * i] for i in data_items[1]]
            # returns a copy of the data, so index starts at zero
            view = slice(0, len(items[1]))
        elif isinstance(items[0], self.ITERABLE_1D) and isinstance(items[1], int):
            # eg .iloc[[1, 2, 3], 0]
            name = columns[items[1]]
            index = tuple(index[i] for i in items[0])
            data = [data[i + step * data_items[i][1]] for i in data_items[i][0]]
            view = slice(0, len(items[0]))

        #####################
        # Returns a DataFrame
        #####################
        elif isinstance(items[0], slice) and isinstance(items[1], slice):
            # e.g. .iloc[1:3, :]
            data = data
            name = columns[items[1]]
            index = index[items[0]]
            view = tuple(data_items)
            step = step
        elif isinstance(items[0], self.ITERABLE_1D) and isinstance(items[1], slice):
            # e.g. .iloc[[1, 2], :]
            # iterate through row
            ndata = []
            for col_index in range(data_items[1].start, data_items[1].stop):
                ndata.extend([data[i + col_index * step] for i in data_items[0]])
            data = ndata
            name = columns[items[1]]
            index = tuple(index[i] for i in items[0])
            step = len(index)
            # retuns a copy, so view starts at zero
            view = (
                slice(0, step,),
                slice(0, len(name),),
            )
        elif isinstance(items[0], slice) and isinstance(items[1], self.ITERABLE_1D):
            # e.g. .iloc[:, [1,2]
            ndata = []
            for i in data_items[1]:
                ndata.extend(
                    data[data_items[0].start + i * step : data_items[0].stop + i * step]
                )
            data = ndata
            index = index[items[0]]
            name = tuple(columns[i] for i in items[1])
            step = len(index)
            # return a copy, view starts at zero
            view = (
                slice(0, step,),
                slice(0, len(name),),
            )
        elif isinstance(items[0], self.ITERABLE_1D) and isinstance(
            items[1], self.ITERABLE_1D
        ):
            # e.g. .iloc[:, [1,2]
            ndata = []
            for i in data_items[1]:
                ndata.extend([data[j + i * step] for j in data_items[0]])
            data = ndata
            index = tuple(index[i] for i in items[0])
            name = tuple(columns[i] for i in items[1])
            step = len(index)
            # return a copy, view starts at zero
            view = (
                slice(0, step,),
                slice(0, len(name),),
            )
        if isinstance(index, tuple) and isinstance(name, (str, int)):
            return Series.from_data(data, index, name, view)
        if isinstance(index, tuple) and isinstance(name, tuple):
            return DataFrame.from_data(data, index, name, view, step)

    def setitem_ser(self, item, value):
        if isinstance(item, slice):
            # eg .iloc[1:3]
            item = slice(
                item.start if item.start is not None else 0,
                item.stop if item.stop is not None else self.obj.view.stop,
            )
            data = self.obj.data
            step = self.obj.view.step
            start = self.obj.view.start + item.start * step
            stop = self.obj.view.start + item.stop * step
            if not isinstance(value, self.ITERABLE_1D + (self.__class__,)):
                value = [value] * len(self.obj)
            for i, val in zip(range(start, stop, step), value):
                data[i] = val

        elif isinstance(item, self.ITERABLE_1D):
            data = self.obj.data
            step = self.obj.view.step if self.obj.view.step is not None else 1
            start = self.obj.view.start
            if not isinstance(value, self.ITERABLE_1D + (self.__class__,)):
                value = [value] * len(self.obj)
            for i, val in zip(item, value):
                data[start + i * step] = val

        else:
            self.obj.data[self.obj.view.start + item * self.obj.view.step] = value

    def getitem_ser(self, item):
        if isinstance(item, slice):
            item = slice(
                item.start if item.start is not None else 0,
                item.stop if item.stop is not None else self.obj.view.stop,
            )
            index = self.obj.index[item]
            view = slice(
                self.obj.view.start + item.start * self.obj.view.step,
                self.obj.view.start + item.stop * self.obj.view.step,
                self.obj.view.step,
            )
            return self.obj.from_data(self.obj.data, index, self.obj.name, view)

        if isinstance(item, self.ITERABLE_1D):
            index = self.obj.index
            index = [index[i] for i in item]
            data = self.obj.values
            data = [data[i] for i in item]
            view = slice(0, len(index), 1)
            return self.obj.from_data(data, index, self.obj.name, view)

        if isinstance(item, int):
            return self.obj.values[item]


class Loc:
    ITERABLE_1D = (list, set, tuple, Series)

    def __init__(self, obj):
        self.obj = obj

    def __getitem__(self, items):
        if isinstance(items, tuple):
            # items arrive as slice and series
            iloc_items = tuple(
                self.obj.index_of(item, axis=i) for (i, item) in enumerate(items)
            )
        else:
            iloc_items = self.obj.index_of(items)
        return self.obj.iloc[iloc_items]

    def __setitem__(self, items, value, what=None):

        if isinstance(items, tuple):
            iloc_items = tuple(
                self.obj.index_of(item, axis=i) for (i, item) in enumerate(items)
            )
        else:
            iloc_items = (self.obj.index_of(items),)

        if isinstance(self.obj, DataFrame):
            # if the index isn't found, add an empty row/column and call it again
            if iloc_items[0] is None:
                # adding a row will break the view. Make a copy.
                self.obj.drop()
                self.obj.add_empty_series(items[0], axis=0)
                self.__setitem__(items, value)
            elif len(items) > 1 and iloc_items[1] is None:
                self.obj.add_empty_series(items[1], axis=1)
                self.__setitem__(items, value)
            else:
                self.obj.iloc.__setitem__(iloc_items, value)
        else:
            if iloc_items[0] is None:
                if isinstance(items, self.ITERABLE_1D + (self.__class__,)):
                    num = len(items)
                else:
                    num = 1
                self.obj.extend(items, num=num)
                self.__setitem__(items, value)
            else:
                self.obj.iloc.__setitem__(iloc_items[0], value)


class DataFrame:
    """
    The only mutable attribute is the data.
    Shape is equal to the view.
    If a row is added, data is recreated and step is also updated.
    If a column is added, data is appended *only* if the dataframe view covers
    the entire dataset (shape equals len index, columns). Otherwise, a copy is made
    view is a tuple of two slices, for the row and column. Steps are not taken into
    account in view, it is high level. This is contrary to Series
    step = len(index) = shape(0) = view[0].stop - view[0].start
    len(columns) = shape(1) = view[1].stop - view[1].start
    """

    ITERABLE_1D = (list, set, tuple, Series)

    def __init__(self, data=None, index=None, columns=None):
        if data is None:
            return
        self.columns = columns
        self.index = index
        self.data = []
        if isinstance(data, dict):
            self.step = len(data[list(data.keys())[0]])
            self.data = list(itertools.chain(*data.values()))
            self.columns = tuple(data.keys())
        elif isinstance(data, list):
            if isinstance(data[0], self.ITERABLE_1D):
                self.step = len(data)
                data = list(zip(*data))
                for item in data:
                    self.data.extend(item)
            elif isinstance(data[0], dict):
                self.columns = []
                for d_dict in data:
                    key, val = next(iter(d_dict.items()))
                    self.columns.append(key)
                    self.data.extend(val)
                self.step = len(val)

        if self.columns is None:
            self.columns = tuple(i for i in range(len(self.data) // self.step))
        else:
            self.columns = tuple(self.columns)
        if self.index is None:
            self.index = tuple(i for i in range(self.step))
        else:
            self.index = tuple(index)
        self.shape = (self.step, len(self.columns))
        self.view = (
            slice(0, self.shape[0]),
            slice(0, self.shape[1]),
        )
        self.iloc = ILoc(self)
        self.loc = Loc(self)

    @classmethod
    def from_data(cls, data, index, columns, view, step):
        self = cls()
        self.data = data
        self.columns = columns
        self.index = index
        self.view = view
        self.step = step
        self.shape = (
            self.view[0].stop - self.view[0].start,
            self.view[1].stop - self.view[1].start,
        )
        self.iloc = ILoc(self)
        self.loc = Loc(self)

        return self

    def __str__(self):
        string = "DataFrame: " + "\n" + str(self.columns) + "\n"
        string += "\n".join(str(d) for d in zip(self.index, self.values))
        return string

    def __getitem__(self, cols):
        # gets here as slice and series
        if isinstance(cols, tuple):
            return self.loc[cols]
        elif isinstance(cols, slice) or is_bool(cols):
            return self.loc[cols, :]
        else:
            return self.loc[:, cols]

    def __setitem__(self, key, value):
        """
        This implementation just adds a new columns
        :param key:
        :param value:
        :return:
        """
        # if it is a single item, call .loc[:, key]
        # if is a slice, call .loc[key, :]
        # if its an iterable call .loc[:, key]
        if isinstance(key, tuple):
            self.loc[key] = value
        elif isinstance(key, slice) or is_bool(key):
            self.loc[key, :] = value
        else:
            self.loc[:, key] = value

    def __delitem__(self, cols):
        self.drop(cols)

    def __lt__(self, other):
        df = self.copy()
        df.data = [item < other for item in df.data]
        return df

    def __le__(self, other):
        df = self.copy()
        df.data = [item <= other for item in df.data]
        return df

    def __gt__(self, other):
        df = self.copy()
        df.data = [item > other for item in df.data]
        return df

    def __ge__(self, other):
        df = self.copy()
        df.data = [item >= other for item in df.data]
        return df

    def __eq__(self, other):
        df = self.copy()
        df.data = [item == other for item in df.data]
        return df

    def __ne__(self, other):
        df = self.copy()
        df.data = [item != other for item in df.data]
        return df

    def __iter__(self):
        return iter(self.columns)

    def drop(self, labels=None):
        """
        Drop both removes a specified column and trims internal data, producing a copy.

        column_index: a column to drop
        :return:
        """
        to_delete = self.view[1].stop
        num = 0
        if isinstance(labels, str):
            to_delete = self.columns.index(labels)
            num = 1

        # build new dataset without the old columns
        data_cols = []

        for col_index in range(self.view[1].start, to_delete):
            data_cols.append(
                self.data[
                    self.view[0].start
                    + col_index * self.step : self.view[0].stop
                    + col_index * self.step
                ]
            )

        for col_index in range(to_delete + num, self.view[1].stop):
            data_cols.append(
                self.data[
                    self.view[0].start
                    + col_index * self.step : self.view[0].stop
                    + col_index * self.step
                ]
            )
        self.data = []
        for col in data_cols:
            self.data.extend(col)

        #    and adjust our indexing
        self.columns = self.columns[0:to_delete] + self.columns[to_delete + num :]
        self.shape = (self.shape[0], self.shape[1] - num)
        self.view = (slice(0, len(self.index)), slice(0, len(self.columns)))
        self.step = len(self.index)

    def copy(self):
        """
        Creates a copy of the dataframe and trims the data with self.drop
        :return:
        """
        df = DataFrame.from_data(
            self.data, self.index, self.columns, self.view, self.step
        )
        df.drop()
        return df

    @property
    def values(self):
        data_rows = []
        for row_index in range(self.view[0].start, self.view[0].stop):
            data_rows.append(
                self.data[
                    row_index
                    + self.step * self.view[1].start : row_index
                    + self.step * self.view[1].stop : self.step
                ]
            )
        return data_rows

    def bound_int_to_df(self, raw_int, axis):
        """
        Transforms an index int to the actual axis index of data

        :param raw_int:
        :param axis:
        :return:
        """
        if axis in [0, "row", "rows"]:
            view_min = self.view[0].start
            view_max = self.view[0].stop
        elif axis in [1, "column", "columns"]:
            view_min = self.view[1].start
            view_max = self.view[1].stop
        else:
            raise UserWarning

        # handle negative ints
        if raw_int < 0:
            start = view_max + raw_int
        else:
            start = view_min + raw_int

        # check bounds
        if start > view_max or start < view_min:
            raise IndexError

        return start

    def bound_slice_to_df(self, raw_slice, axis):
        """
        Transforms a slice to the actual axis view for the data
        :param raw_slice:
        :return:
        """
        if axis in [0, "row", "rows"]:
            view_start = self.view[0].start
            view_stop = self.view[0].stop
        elif axis in [1, "column", "columns"]:
            view_start = self.view[1].start
            view_stop = self.view[1].stop
        else:
            pass

        if raw_slice.start:
            if raw_slice.start < 0:
                # if its negative, subtract from the end or the start
                start = max(view_stop + raw_slice.start, view_start)
            else:
                start = raw_slice.start + view_start
        else:
            start = view_start

        if raw_slice.stop:
            if raw_slice.stop < 0:
                stop = max(view_stop + raw_slice.stop, view_start)
            else:
                stop = view_start + raw_slice.stop
        else:
            stop = view_stop
        return slice(start, stop,)

    def bound_iterable_to_df(self, raw_iter, axis):
        """
        Converts indicies to the actual data indicies
        :param raw_iter:
        :param axis:
        :return:
        """
        print("raw iter", raw_iter)
        return [self.bound_int_to_df(item, axis) for item in raw_iter]

    def convert_slice(self, raw_slice, axis):
        """
        Removes the None from the slice and replaces it with length of rows or columns.
        doesn't adjust based on view
        :param raw_slice:
        :param axis:
        :return:
        """
        if axis in [0, "row", "rows"]:
            max_stop = len(self.index)
        elif axis in [1, "columns", "cols", "col"]:
            max_stop = len(self.columns)

        if not raw_slice.start:
            start = 0
        elif raw_slice.start < 0:
            start = max(0, max_stop + raw_slice.start)
        else:
            start = raw_slice.start

        if not raw_slice.stop:
            stop = max_stop
        elif raw_slice.stop < 0:
            stop = max(0, max_stop + raw_slice.stop)
        else:
            stop = raw_slice.stop
        return slice(start, stop)

    def is_view(self):
        return self.shape[0] != self.step or self.shape[1] != len(self.data) / self.step

    def index_of(self, item, axis=0):
        if axis in [0, "rows", "row"]:
            names = self.index
        else:
            names = self.columns
        if isinstance(item, self.ITERABLE_1D):
            # bypass for boolean
            if is_bool(item):
                return item
            return [names.index(i) for i in item]
        elif isinstance(item, slice):
            start = None if item.start is None else names.index(item.start)
            stop = None if item.stop is None else names.index(item.stop)
            return slice(start, stop)
        else:
            try:
                return names.index(item)
            except:
                return None

    def add_empty_series(self, name, axis=0):
        # if we are adding a row
        # cannot add to a view
        if self.is_view():
            self.drop()
        if axis == 0:
            self.index = self.index + (name,)
            ndata = []
            for i in range(self.shape[1]):
                ndata.extend(self.data[i * self.step : (i + 1) * self.step] + [nan])
            self.data = ndata
            self.shape = (self.shape[0] + 1, self.shape[1])
            self.view = (slice(self.view[0].start, self.view[0].stop + 1), self.view[1])
            self.step = self.step + 1
        if axis == 1:
            self.columns = self.columns + (name,)
            self.data = self.data + [nan] * self.shape[0]
            self.shape = (self.shape[0], self.shape[1] + 1)
            self.view = (self.view[0], slice(self.view[1].start, self.view[1].stop + 1))
        # if we are adding a column


def is_bool(key):
    try:
        if isinstance(key[0], bool) or (
            isinstance(key, Series) and isinstance(key.iloc[0], bool)
        ):
            return True
        return False
    except:
        return False


def clean_slices(phase, info):
    if phase == "stop":
        return
    obj_dict = {}
    data_dict = {}
    for obj in gc.get_objects():
        if isinstance(obj, (Series, DataFrame)):
            d_id = id(obj.data)
            data_dict[d_id] = obj.data
            try:
                obj_dict[d_id].append(obj)
            except:
                obj_dict[d_id] = [obj]

    # check if the series is part of a dataframe. if so, skip it.

    for d_id, objs in obj_dict.items():
        # find minimum start or maximum stop
        start = objs[0].view.start
        stop = objs[0].view.stop
        for obj in objs[1:]:
            # early stopping if both are None. Data is still in use somewhere
            if start is None and stop is None:
                break
            if (start is not None) and (
                (obj.view.start is None) or obj.view.start < start
            ):
                start = obj.view.start
            if stop is not None and ((obj.view.stop is None) or obj.view.stop > stop):
                stop = obj.view.stop
        # if both are None, don't do anything. No trimming necessary
        if start is None and stop is None:
            continue
        if start is None:
            start = 0
        # adjust the data and view for each by trimming data and subtracting the start
        data_dict[d_id][:] = data_dict[d_id][start:stop]
        for obj in objs:
            new_start = new_stop = None
            if obj.view.start is not None:
                new_start = obj.view.start - start
            if obj.view.stop is not None:
                new_stop = obj.view.stop - start
            obj.view = slice(new_start, new_stop)
            obj.index[:] = obj.index[start:stop]
