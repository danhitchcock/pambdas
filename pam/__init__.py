from copy import copy
import itertools
import gc


class Series:
    @classmethod
    def from_data(cls, data, index, name=None, view=slice(None, None)):
        self = cls()
        self.data = data
        self.index = index
        self.name = name
        self.view = view
        self.iloc = ILoc(self)
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
        if data and not index:
            index = list(range(len(data)))
        self.data = data
        self.view = view
        self.index = index
        self.name = name
        self.iloc = ILoc(self)

    def __setitem__(self, key, value):
        # step = self.view.step if self.view.step is not None else 1
        # start = self.view.start if self.view.start is not None else 0
        if isinstance(key, (list, tuple, set)):
            # lose the reference
            pass
        elif isinstance(key, int):
            self.data[self.view.start + key * self.view.step] = value
        elif isinstance(key, slice):
            key_start = key.start if key.start is not None else 0
            key_stop = key.stop if key.stop is not None else len(value)
            self.data[
                self.view.start
                + key_start * self.view.step : self.view.start
                + key_stop * self.view.step : self.view.step
            ] = value

    def __getitem__(self, item):
        if isinstance(item, slice):
            key_start = item.start if item.start is not None else 0
            key_stop = item.stop if item.stop is not None else self.view.stop
            step = self.view.step if self.view.step is not None else 1

            b = copy(self)
            b.view = slice(
                self.view.start + key_start * step,
                self.view.start + key_stop * step,
                step,
            )
            return b
        elif isinstance(item, int):
            return self.data[item]

    def __str__(self):
        return (
            "Series Name: "
            + str(self.name)
            + "\n"
            + "\n".join(
                "%s: %s" % (item[0], item[1])
                for item in zip(
                    self.index, self.data[self.view.start : self.view.stop],
                )
            )
        )

    def __repr__(self):
        return str(self)

    def __iter__(self):
        return iter(self.data[self.view])

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
        ser = self.copy()
        ser.data = [item == other for item in ser.data]
        return ser

    def __ne__(self, other):
        ser = self.copy()
        ser.data = [item != other for item in ser.data]
        return ser

    def drop(self, labels=None):
        """
        Essentially produces a trimmed copy of the series

        column_index: a column to drop
        :return:
        """
        to_delete = self.view.stop
        num = 0
        if labels in self.name.index:
            to_delete = self.name.index(labels)
            num = 1

        self.data = (
            self.data[self.view][0:to_delete]
            + self.data[self.view][to_delete + num : to_delete]
        )
        self.index = self.index[0:to_delete] + self.index[to_delete + num :]

        #    and adjust our indexing
        self.shape = (self.shape[0], self.shape[1] - num)
        self.view = slice(0, len(self.index), 1)
        return self

    def copy(self):
        ser = self.from_data(
            self.data[self.view], self.index, self.name, slice(0, len(self.index))
        )
        return ser

    def __len__(self):
        return len(self.index)


class ILoc:
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

    def getitem_ser(self, items):
        print(items)

    def setitem_df(self, items, value):
        data = self.obj.data
        step = self.obj.step
        shape = self.obj.shape

        # tuples are packages indices
        if isinstance(items, tuple):
            n_indexers = len(items)
            items = list(items)
        else:
            n_indexers = 1
            items = [items]

        for i, item in enumerate(items):
            if isinstance(item, self.ITERABLE_1D):
                if item[0] is True or item[0] is False:
                    items[i] = [i for i in range(len(item)) if item[i]]

        if n_indexers == 1:
            print("1 indexer is not supported yet")

        # convert boolean to lists
        if n_indexers == 2:
            #################
            # Returns an item
            #################
            if isinstance(items[0], int) and isinstance(items[1], int):
                # eg [1, 0]
                data[items[0] + shape[0] * items[1]] = value
            ##################
            # Returns a Series
            ##################
            if isinstance(items[0], slice) and isinstance(items[1], int):
                # eg [1:3, 0]
                items[0] = slice(
                    items[0].start if items[0].start is not None else 0,
                    items[0].stop if items[0].stop is not None else step,
                )
                data[
                    items[0].start + step * items[1] : items[0].stop + items[1] * step
                ] = value

            if isinstance(items[0], int) and isinstance(items[1], slice):
                # eg .iloc[0, 1:3]
                items[1] = slice(
                    items[1].start if items[1].start is not None else 0,
                    items[1].stop if items[1].stop is not None else shape[1],
                )
                start = items[0] + step * items[1].start
                stop = items[0] + step * items[1].stop
                for i, val in zip(range(start, stop, step), value):
                    data[i] = val
            if isinstance(items[0], int) and isinstance(items[1], self.ITERABLE_1D):
                # eg .iloc[0, [1, 2, 3]]
                for i, val in zip(items[1], value):
                    data[items[0] + step * i] = val
            if isinstance(items[0], (list, set, tuple)) and isinstance(items[1], int):
                # eg .iloc[[1, 2, 3], 0]
                for i, val in zip(items[0], value):
                    data[i + step * items[1]] = val

            #####################
            # Returns a DataFrame
            #####################
            # warning: everything below is very messy.

            if isinstance(items[0], slice) and isinstance(items[1], slice):
                # e.g. .iloc[1:3, :]
                items[0] = slice(
                    items[0].start if items[0].start is not None else 0,
                    items[0].stop if items[0].stop is not None else step,
                )
                items[1] = slice(
                    items[1].start if items[1].start is not None else 0,
                    items[1].stop if items[1].stop is not None else shape[1],
                )
                if not isinstance(value, self.ITERABLE_1D):
                    pass
                # there is almost certainly a better way to do this
                k = 0
                value = list(itertools.chain.from_iterable(value))
                for i in range(items[0].start, items[0].stop):
                    for j in range(items[1].start, items[1].stop):
                        data[i + j * step] = value[k]
                        k += 1
            if isinstance(items[0], self.ITERABLE_1D) and isinstance(items[1], slice):
                # e.g. .iloc[[1, 2], :]
                items[1] = slice(
                    items[1].start if items[1].start is not None else 0,
                    items[1].stop if items[1].stop is not None else shape[1],
                )
                # there is almost certainly a better way to do this
                k = 0
                value = list(itertools.chain.from_iterable(value))
                for i in items[0]:
                    for j in range(items[1].start, items[1].stop):
                        data[i + j * step] = value[k]
                        k += 1
            if isinstance(items[0], (slice)) and isinstance(items[1], self.ITERABLE_1D):
                # e.g. .iloc[:, [1,2]
                items[0] = slice(
                    items[0].start if items[0].start is not None else 0,
                    items[0].stop if items[0].stop is not None else step,
                )
                k = 0
                value = list(itertools.chain.from_iterable(value))
                for i in range(items[0].start, items[0].stop):
                    for j in items[1]:
                        data[i + j * step] = value[k]
                        k += 1
            if isinstance(items[0], self.ITERABLE_1D) and isinstance(
                items[1], self.ITERABLE_1D
            ):
                # e.g. .iloc[:, [1,2]
                k = 0
                value = list(itertools.chain.from_iterable(value))
                for i in items[0]:
                    for j in items[1]:
                        data[i + j * step] = value[k]
                        k += 1

    def getitem_df(self, items):
        data = self.obj.data
        index = self.obj.index
        columns = self.obj.columns
        step = self.obj.step
        view = self.obj.view
        shape = self.obj.shape

        # tuples are packages indices
        if isinstance(items, tuple):
            n_indexers = len(items)
            items = list(items)
        else:
            n_indexers = 1
            items = [items]

        for i, item in enumerate(items):
            if isinstance(item, self.ITERABLE_1D):
                if item[0] is True or item[0] is False:
                    items[i] = [i for i in range(len(item)) if item[i]]

        if n_indexers == 1:
            print("1 indexer is not supported yet")

        # convert boolean to lists
        if n_indexers == 2:
            #################
            # Returns an item
            #################
            if isinstance(items[0], int) and isinstance(items[1], int):
                # eg [1, 0]
                return data[items[0] + shape[0] * items[1]]
            ##################
            # Returns a Series
            ##################
            if isinstance(items[0], slice) and isinstance(items[1], int):
                # eg [1:3, 0]
                items[0] = slice(
                    items[0].start if items[0].start is not None else 0,
                    items[0].stop if items[0].stop is not None else step,
                )
                index = index[items[0]]
                name = columns[items[1]]
                data = data
                view = slice(items[0].start, items[0].stop, 1)
            if isinstance(items[0], int) and isinstance(items[1], slice):
                # eg .iloc[0, 1:3]
                items[1] = slice(
                    items[1].start if items[1].start is not None else 0,
                    items[1].stop if items[1].stop is not None else shape[1],
                )
                name = index[items[0]]
                index = columns[items[1]]
                start = items[0] + step * items[1].start
                stop = items[0] + step * items[1].stop
                view = slice(start, stop, step)
                data = data
            if isinstance(items[0], int) and isinstance(items[1], self.ITERABLE_1D):
                # eg .iloc[0, [1, 2, 3]]
                name = index[items[0]]
                index = tuple(columns[i] for i in items[1])
                data = [data[items[0] + step * i] for i in items[1]]
                view = slice(0, len(items[1]))
            if isinstance(items[0], (list, set, tuple)) and isinstance(items[1], int):
                # eg .iloc[[1, 2, 3], 0]
                name = columns[items[1]]
                index = tuple(index[i] for i in items[0])
                data = [data[i + step * items[1]] for i in items[0]]
                view = slice(0, len(items[0]))

            #####################
            # Returns a DataFrame
            #####################
            if isinstance(items[0], slice) and isinstance(items[1], slice):
                # e.g. .iloc[1:3, :]
                items[0] = slice(
                    items[0].start if items[0].start is not None else 0,
                    items[0].stop if items[0].stop is not None else step,
                )
                items[1] = slice(
                    items[1].start if items[1].start is not None else 0,
                    items[1].stop if items[1].stop is not None else shape[1],
                )
                data = data
                name = columns[items[1]]
                index = index[items[0]]
                view = (
                    slice(
                        view[0].start + items[0].start, view[0].start + items[0].stop,
                    ),
                    slice(
                        view[1].start + items[1].start, view[1].start + items[1].stop,
                    ),
                )
                step = step
            if isinstance(items[0], self.ITERABLE_1D) and isinstance(items[1], slice):
                # e.g. .iloc[[1, 2], :]
                items[1] = slice(
                    items[1].start if items[1].start is not None else 0,
                    items[1].stop if items[1].stop is not None else shape[1],
                )
                ndata = []
                for col_index in range(items[1].start, items[1].stop):
                    ndata.extend([data[i + col_index * step] for i in items[0]])
                data = ndata
                name = columns[items[1]]
                index = tuple(index[i] for i in items[0])
                step = len(index)
                view = (
                    slice(0, step,),
                    slice(0, len(name),),
                )
            if isinstance(items[0], (slice)) and isinstance(items[1], self.ITERABLE_1D):
                # e.g. .iloc[:, [1,2]
                items[0] = slice(
                    items[0].start if items[0].start is not None else 0,
                    items[0].stop if items[0].stop is not None else shape[0],
                )
                ndata = []
                for i in items[1]:
                    ndata.extend(
                        data[items[0].start + i * step : items[0].stop + i * step]
                    )
                data = ndata
                index = index[items[0]]
                name = tuple(columns[i] for i in items[1])
                step = len(index)
                view = (
                    slice(0, step,),
                    slice(0, len(name),),
                )
            if isinstance(items[0], self.ITERABLE_1D) and isinstance(
                items[1], self.ITERABLE_1D
            ):
                # e.g. .iloc[:, [1,2]
                ndata = []
                for i in items[1]:
                    ndata.extend([data[j + i * step] for j in items[0]])
                data = ndata
                index = tuple(index[i] for i in items[0])
                name = tuple(columns[i] for i in items[1])
                step = len(index)
                view = (
                    slice(0, step,),
                    slice(0, len(name),),
                )
            if isinstance(index, tuple) and isinstance(name, (str, int)):
                return Series.from_data(data, index, name, view)
            if isinstance(index, tuple) and isinstance(name, tuple):
                return DataFrame.from_data(data, index, name, view, step)


class Loc:
    ITERABLE_1D = (list, set, tuple, Series)

    def __init__(self, obj):
        self.obj = obj

    def __getitem__(self, items):
        items = self.items_to_iloc(items)
        return self.obj.iloc[items]

    def __setitem__(self, items, value, what=None):
        items = self.items_to_iloc(items)
        self.obj.iloc.__setitem__(items, value)

    def items_to_iloc(self, items):
        index = self.obj.index
        columns = self.obj.columns
        # tuples are packages indices
        if isinstance(items, tuple):
            n_indexers = len(items)
            items = list(items)
        else:
            n_indexers = 1
            items = [items]
        if n_indexers == 1:
            print("loc not supported for 1 item yet")
        if n_indexers == 2:
            for i, (item, names) in enumerate(zip(list(items), [index, columns])):
                if isinstance(item, self.ITERABLE_1D):
                    # bypass for boolean
                    if isinstance(item[0], bool):
                        continue
                    items[i] = [names.index(i) for i in item]
                elif isinstance(item, slice):
                    start = None if item.start is None else names.index(item.start)
                    stop = None if item.stop is None else names.index(item.stop)
                    items[i] = slice(start, stop)
                else:
                    items[i] = names.index(item)
        return tuple(items)


class DataFrame:
    """
    The only mutable attribute is the data.
    Shape is equal to the view.
    If a row is added, data is recreated and step is also updated.
    If a column is added, data is appended *only* if the dataframe view covers
    the entire dataset (shape equals len index, columns). Otherwise, a copy is made
    """

    ITERABLE_1D = (list, set, tuple, Series)

    def __init__(self, data=None, index=None, columns=None):
        if data is None:
            return
        self.columns = index
        self.index = columns
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
        data_cols = []
        for col_index in range(self.view[1].start, self.view[1].stop):
            data_cols.append(
                self.data[
                    self.view[0].start
                    + col_index * self.step : self.view[0].stop
                    + col_index * self.step
                ]
            )
        string += "\n".join(str(d) for d in zip(self.index, *data_cols))
        return string

    def __getitem__(self, cols):
        return self.loc[:, cols]

    def __setitem__(self, key, value):
        # in the future, will call iloc __setitem__
        if isinstance(value, (list, set, tuple)):
            # check if our view is the whole dataframe. If not, make a copy
            if (
                self.shape[0] != self.step
                or self.shape[1] != len(self.data) / self.step
            ):
                self.drop()
            self.data.extend(value)
            self.columns = self.columns + (key,)
            self.shape = (self.shape[0], self.shape[1] + 1)
            self.view = (self.view[0], slice(self.view[1].start, self.view[1].stop + 1))

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
        df = DataFrame.from_data(
            self.data, self.index, self.columns, self.view, self.step
        )
        df.drop()
        return df


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
