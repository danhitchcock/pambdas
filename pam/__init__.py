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
                for item in zip(self.index, self.data[self.view.start : self.view.sto],)
            )
        )

    def __repr__(self):
        return str(self)

    def __iter__(self, all=False):
        return iter([d for d in self.data[self.view]])

    def values(self):
        return self.data[self.view]


class ILoc:
    def __init__(self, obj):
        self.obj = obj

    def __getitem__(self, items):
        self.data = self.obj.data
        self.index = self.obj.index
        self.columns = self.obj.columns
        self.step = self.obj.step
        self.view = self.obj.view
        self.shape = self.obj.shape

        items = list(items)
        # convert boolean to lists
        if len(items) >= 2:
            for i, item in enumerate(items):
                if isinstance(item, (list, set, tuple)):
                    if item[0] is True or item[0] is False:
                        items[i] = [i for i in range(len(item)) if item[i]]

            #################
            # Returns an item
            #################
            if isinstance(items[0], int) and isinstance(items[1], int):
                # eg [1, 0]
                return self.data(items[0] + self.data.shape[0] * items[1])
            ##################
            # Returns a Series
            ##################
            if isinstance(items[0], slice) and isinstance(items[1], int):
                # eg [1:3, 0]
                items[0] = slice(
                    items[0].start if items[0].start is not None else 0,
                    items[0].stop if items[0].stop is not None else self.step,
                )
                index = self.index[items[0]]
                name = self.columns[items[1]]
                data = self.data
                view = slice(items[0].start, items[0].stop, 1)
            if isinstance(items[0], int) and isinstance(items[1], slice):
                # eg .iloc[0, 1:3]
                items[1] = slice(
                    items[1].start if items[1].start is not None else 0,
                    items[1].stop if items[1].stop is not None else self.shape[1],
                )
                name = self.index[items[0]]
                index = self.columns[items[1]]
                start = items[0] + self.step * items[1].start
                stop = items[0] + self.step * items[1].stop
                view = slice(start, stop, self.step)
                data = self.data
            if isinstance(items[0], int) and isinstance(items[1], (list, set, tuple)):
                # eg .iloc[0, [1, 2, 3]]
                name = self.index[items[0]]
                index = tuple(self.columns[i] for i in items[1])
                data = [self.data[items[0] + self.step * i] for i in items[1]]
                view = slice(0, len(items[1]))
            if isinstance(items[0], (list, set, tuple)) and isinstance(items[1], int):
                # eg .iloc[[1, 2, 3], 0]
                name = self.columns[items[1]]
                index = tuple(self.index[i] for i in items[0])
                data = [self.data[i + self.step * items[1]] for i in items[0]]
                view = slice(0, len(items[0]))

            #####################
            # Returns a DataFrame
            #####################
            if isinstance(items[0], slice) and isinstance(items[1], slice):
                # e.g. .iloc[1:3, :]
                items[0] = slice(
                    items[0].start if items[0].start is not None else 0,
                    items[0].stop if items[0].stop is not None else self.step,
                )
                items[1] = slice(
                    items[1].start if items[1].start is not None else 0,
                    items[1].stop if items[1].stop is not None else self.shape[1],
                )
                data = self.data
                name = self.columns[items[1]]
                index = self.index[items[0]]
                print(type(index), type(name))
                view = (
                    slice(
                        self.view[0].start + items[0].start,
                        self.view[0].start + items[0].stop,
                    ),
                    slice(
                        self.view[1].start + items[1].start,
                        self.view[1].start + items[1].stop,
                    ),
                )
                step = self.step
            if isinstance(items[0], (list, set, tuple)) and isinstance(items[1], slice):
                # e.g. .iloc[[1, 2], :]
                items[1] = slice(
                    items[1].start if items[1].start is not None else 0,
                    items[1].stop if items[1].stop is not None else self.shape[1],
                )
                data = []
                for col_index in range(items[1].start, items[1].stop):
                    # print(col_index)
                    data.extend(
                        [self.data[i + col_index * self.step] for i in items[0]]
                    )
                name = self.columns[items[1]]
                index = tuple(self.index[i] for i in items[0])
                step = len(index)
                view = (
                    slice(0, step,),
                    slice(0, len(name),),
                )
            if isinstance(items[0], (slice)) and isinstance(
                items[1], (list, set, tuple)
            ):
                # e.g. .iloc[:, [1,2]
                items[0] = slice(
                    items[0].start if items[0].start is not None else 0,
                    items[0].stop if items[0].stop is not None else self.shape[0],
                )
                data = []
                for i in items[1]:
                    # print(col_index)
                    data.extend(
                        self.data[
                            items[0].start
                            + i * self.step : items[0].stop
                            + i * self.step
                        ]
                    )
                index = self.index[items[0]]
                name = tuple(self.columns[i] for i in items[1])
                step = len(index)
                view = (
                    slice(0, step,),
                    slice(0, len(name),),
                )
            if isinstance(items[0], (list, set, tuple)) and isinstance(
                items[1], (list, set, tuple)
            ):
                # e.g. .iloc[:, [1,2]
                data = []
                for i in items[1]:
                    data.extend([self.data[j + i * self.step] for j in items[0]])

                index = tuple(self.index[i] for i in items[0])
                name = tuple(self.columns[i] for i in items[1])
                step = len(index)
                view = (
                    slice(0, step,),
                    slice(0, len(name),),
                )

            if isinstance(index, tuple) and isinstance(name, (str, int)):
                return Series.from_data(data, index, name, view)
            if isinstance(index, tuple) and isinstance(name, tuple):
                return DataFrame.from_data(data, index, name, view, step)

    def __setitem__(self, items, value):

        self.obj.data[
            self.obj.view[0].start
            + items[0]
            + self.obj.step * (self.obj.view[1].start + items[1])
        ] = value


class DataFrame:
    """
    The only mutable attribute is the data.
    Shape is equal to the view.
    If a row is added, data is recreated and step is also updated.
    If a column is added, data is appended *only* if the dataframe view covers
    the entire dataset (shape equals len index, columns). Otherwise, a copy is made
    """

    def __init__(self, data=None, index=None, columns=None):
        if data is None:
            return
        if isinstance(data, dict):
            self.step = len(data[list(data.keys())[0]])
            self.data = list(itertools.chain(*data.values()))
            self.columns = tuple(data.keys())
            self.index = tuple(i for i in range(self.step))
            self.shape = (self.step, len(self.columns))
            self.view = (
                slice(0, self.shape[0]),
                slice(0, self.shape[1]),
            )
        self.iloc = ILoc(self)

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
        # in the future, will call .loc[:, cols]
        cols_index = self.columns.index(cols)
        return Series.from_data(
            self.data, self.index, cols, slice(cols_index, cols_index + self.shape[0])
        )

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

    def drop(self, labels=None):
        """
        Essentially produces a trimmed copy of the dataframe

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
