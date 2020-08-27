from copy import copy
import itertools

from .other_stuff import is_bool


class ILocDF:
    """
    ILoc indexer for dataframes
    """

    def __init__(self, obj):
        self.obj = obj

    def __getitem__(self, items):
        data = self.obj.data
        index = self.obj.index
        columns = self.obj.columns
        step = self.obj.step
        view = self.obj.view
        # if it's a tuple, its multiple indicies. Otherwise, its one item, so
        # make a dummy index
        if isinstance(items, tuple):
            items = list(items)
        else:
            items = [items, slice(None, None)]
        data_items = copy(items)

        # convert to bool, or bound
        for i, item in enumerate(items):
            if isinstance(item, self.obj.ITERABLE_1D):
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
        elif isinstance(items[0], int) and isinstance(items[1], self.obj.ITERABLE_1D):
            # eg .iloc[0, [1, 2, 3]]
            name = index[items[0]]
            index = tuple(columns[i] for i in items[1])
            data = [data[data_items[0] + step * i] for i in data_items[1]]
            # returns a copy of the data, so index starts at zero
            view = slice(0, len(items[1]))
        elif isinstance(items[0], self.obj.ITERABLE_1D) and isinstance(items[1], int):
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
        elif isinstance(items[0], self.obj.ITERABLE_1D) and isinstance(items[1], slice):
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
                slice(
                    0,
                    step,
                ),
                slice(
                    0,
                    len(name),
                ),
            )
        elif isinstance(items[0], slice) and isinstance(items[1], self.obj.ITERABLE_1D):
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
                slice(
                    0,
                    step,
                ),
                slice(
                    0,
                    len(name),
                ),
            )
        elif isinstance(items[0], self.obj.ITERABLE_1D) and isinstance(
            items[1], self.obj.ITERABLE_1D
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
                slice(
                    0,
                    step,
                ),
                slice(
                    0,
                    len(name),
                ),
            )
        if isinstance(index, tuple) and isinstance(name, (str, int)):
            return self.obj.series_from_data(data, index, name, view)
        if isinstance(index, tuple) and isinstance(name, tuple):
            return self.obj.from_data(data, index, name, view, step)

    def __setitem__(self, items, value):
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
            if isinstance(item, self.obj.ITERABLE_1D):
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
            if not isinstance(value, self.obj.ITERABLE_1D):
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
            if not isinstance(value, self.obj.ITERABLE_1D):
                value = [value] * (data_items[1].stop - data_items[1].start)

            for i, val in zip(range(start, stop, step), value):
                data[i] = val
        if isinstance(data_items[0], int) and isinstance(
            data_items[1], self.obj.ITERABLE_1D
        ):
            # eg .iloc[0, [1, 2, 3]]
            if not isinstance(value, self.obj.ITERABLE_1D):
                value = [value] * (len(data_items[1]))
            for i, val in zip(items[1], value):
                data[data_items[0] + step * i] = val

        if isinstance(data_items[0], self.obj.ITERABLE_1D) and isinstance(
            data_items[1], int
        ):
            # eg .iloc[[1, 2, 3], 0]
            if not isinstance(value, self.obj.ITERABLE_1D):
                value = [value] * (len(data_items[0]))
            for i, val in zip(data_items[0], value):
                data[i + step * data_items[1]] = val

        #####################
        # Returns a DataFrame
        #####################
        # warning: everything below is very messy.

        if isinstance(data_items[0], slice) and isinstance(data_items[1], slice):
            # e.g. .iloc[1:3, :]
            # there is almost certainly a better way to do this
            k = 0
            if isinstance(value, self.obj.ITERABLE_1D):
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
        if isinstance(data_items[0], self.obj.ITERABLE_1D) and isinstance(
            data_items[1], slice
        ):
            # e.g. .iloc[[1, 2], :]
            # there is almost certainly a better way to do this
            k = 0
            if isinstance(value, self.obj.ITERABLE_1D):
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
            data_items[1], self.obj.ITERABLE_1D
        ):
            # e.g. .iloc[:, [1,2]
            k = 0
            if isinstance(value, self.obj.ITERABLE_1D):
                value = list(itertools.chain.from_iterable(value))
            else:
                value = [value] * (
                    (data_items[0].stop - data_items[0].start) * len(data_items[1])
                )

            for i in range(data_items[0].start, data_items[0].stop):
                for j in data_items[1]:
                    data[i + j * step] = value[k]
                    k += 1
        if isinstance(data_items[0], self.obj.ITERABLE_1D) and isinstance(
            data_items[1], self.obj.ITERABLE_1D
        ):
            # e.g. .iloc[:, [1,2]
            k = 0
            if isinstance(value, self.obj.ITERABLE_1D):
                value = list(itertools.chain.from_iterable(value))
            else:
                value = [value] * (len(data_items[0]) * len(data_items[1]))
            for i in data_items[0]:
                for j in data_items[1]:
                    data[i + j * step] = value[k]
                    k += 1


class ILocSer:
    """
    This could probably be split into two class -- a dataframe indexer and a Series
    indexer
    """

    ITERABLE_1D = (list, set, tuple)

    def __init__(self, obj):
        self.obj = obj

    def __getitem__(self, item):
        if isinstance(item, tuple):
            item = item[0]

        if isinstance(item, slice):
            item = slice(
                item.start if item.start is not None else 0,
                item.stop if item.stop is not None else len(self.obj),
            )
            view = self.obj.bound_slice(item)
            index = self.obj.index[item]
            return self.obj.from_data(self.obj.data, index, self.obj.name, view)

        if isinstance(item, self.ITERABLE_1D + (self.obj.__class__,)):
            if is_bool(item):
                item = [i for i, val in enumerate(item) if val]

            index = self.obj.index
            index = [index[i] for i in item]
            data = self.obj.values
            data = [data[i] for i in item]
            view = slice(0, len(index), 1)
            return self.obj.from_data(data, index, self.obj.name, view)

        if isinstance(item, int):
            return self.obj.values[item]

    def __setitem__(self, item, value):
        # if there are multiple args, just take the first item
        if isinstance(item, tuple):
            item = item[0]

        # convert to bool, or bound
        if isinstance(item, self.ITERABLE_1D + (self.obj.__class__,)):
            # if it's a boolean
            if is_bool(item):
                item = [i for i, val in enumerate(item) if val]
            data_item = self.obj.bound_iterable(item)
            if not isinstance(value, self.ITERABLE_1D + (self.obj.__class__,)):
                value = [value] * len(self.obj)

            for i, val in zip(data_item, value):
                self.obj.data[i] = val

        elif isinstance(item, slice):
            item = slice(
                item.start if item.start is not None else 0,
                item.stop if item.stop is not None else len(self.obj),
            )
            data_item = self.obj.bound_slice(item)
            if not isinstance(value, self.ITERABLE_1D + (self.obj.__class__,)):
                value = [value] * ((data_item.stop - data_item.start) // data_item.step)

            self.obj.data[data_item] = value

        else:
            data_item = self.obj.bound_int(item)
            self.obj.data[data_item] = value


class Loc:
    """
    Loc indexer for both DF and Series
    """

    ITERABLE_1D = (list, set, tuple)

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


class LocSer(Loc):
    def __init__(self, obj):
        super().__init__(obj)

    def __setitem__(self, items, value, what=None):
        if isinstance(items, tuple):
            iloc_items = tuple(
                self.obj.index_of(item, axis=i) for (i, item) in enumerate(items)
            )
        else:
            iloc_items = (self.obj.index_of(items),)

        if iloc_items[0] is None:
            if isinstance(items, self.ITERABLE_1D + (self.obj.__class__,)):
                num = len(items)
            else:
                num = 1
            self.obj.extend(items, num=num)
            self.__setitem__(items, value)
        else:
            self.obj.iloc.__setitem__(iloc_items[0], value)


class LocDF(Loc):
    def __init__(self, obj):
        super().__init__(obj)

    def __setitem__(self, items, value, what=None):
        if isinstance(items, tuple):
            iloc_items = tuple(
                self.obj.index_of(item, axis=i) for (i, item) in enumerate(items)
            )
        else:
            iloc_items = (self.obj.index_of(items),)
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
