import pam


def test_init_series():
    # test dictionary, default index
    ser = pam.Series([1, 100, 1000, 10000])
    assert ser.index == (0, 1, 2, 3)
    assert ser.values == [1, 100, 1000, 10000]

    ser = pam.Series([1, 100, 1000, 10000], index=[10, 11, 12, 13])
    assert ser.index == (10, 11, 12, 13)
    assert ser.values == [1, 100, 1000, 10000]


def test_init_dataframe():
    # test dictionary, default index
    df = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    assert df.index == (0, 1, 2)
    assert df.columns == ("one", "two")
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    # test dictionary, custom index
    df = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]}, index=["a", "b", "c"])
    assert df.index == ("a", "b", "c")
    assert df.columns == ("one", "two")
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    # test list of dicts, custom index
    df = pam.DataFrame([{"one": [1, 2, 3]}, {"two": [2, 3, 4]}])
    assert df.index == (0, 1, 2)
    assert df.columns == ("one", "two")
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    # test list of dicts, custom index
    df = pam.DataFrame([{"one": [1, 2, 3]}, {"two": [2, 3, 4]}], index=["a", "b", "c"])
    assert df.index == ("a", "b", "c")
    assert df.columns == ("one", "two")
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    # test rows
    df = pam.DataFrame([[1, 2], [2, 3], [3, 4]])
    assert df.index == (0, 1, 2)
    assert df.columns == (0, 1)
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    # test rows with custom index
    df = pam.DataFrame([[1, 2], [2, 3], [3, 4]], index=["a", "b", "c"])
    assert df.index == ("a", "b", "c")
    assert df.columns == (0, 1)
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    # test rows with custom index
    df = pam.DataFrame([[1, 2], [2, 3], [3, 4]], columns=["one", "two"])
    assert df.index == (0, 1, 2)
    assert df.columns == ("one", "two")
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    # test rows with custom index and column
    df = pam.DataFrame(
        [[1, 2], [2, 3], [3, 4]], columns=["one", "two"], index=["a", "b", "c"]
    )
    assert df.index == ("a", "b", "c")
    assert df.columns == ("one", "two")
    assert df.values == [[1, 2], [2, 3], [3, 4]]


def test_bound_methods():
    long_df = pam.DataFrame(
        {
            "a": [0, 0, 0, 0, 0],
            "one": [0, 1, 2, 3, 4],
            "two": [1, 2, 3, 4, 5],
            "three": [2, 3, 4, 5],
            "b": [0, 0, 0, 0, 0],
        }
    )

    df = long_df.iloc[2:-1, 1:-1]
    assert long_df.view == (slice(0, 5), slice(0, 5))
    assert df.view == (slice(2, 4), slice(1, 4))
    assert df.bound_slice_to_df(slice(None, None), axis=0) == slice(2, 4)
    assert df.bound_slice_to_df(slice(0, 1), axis=0) == slice(2, 3)
    assert df.bound_slice_to_df(slice(-2, -1), axis=0) == slice(2, 3)
    assert df.bound_int_to_df(0, axis=0) == 2
    assert df.bound_int_to_df(-1, axis=0) == 3
    assert df.bound_iterable_to_df([0, 1, -1], axis=0) == [2, 3, 3]

    assert df.bound_slice_to_df(slice(None, None), axis=1) == slice(1, 4)
    assert df.bound_slice_to_df(slice(0, 1), axis=1) == slice(1, 2)
    assert df.bound_slice_to_df(slice(-2, -1), axis=1) == slice(2, 3)
    assert df.bound_int_to_df(0, axis=1) == 1
    assert df.bound_int_to_df(-1, axis=1) == 3
    assert df.bound_iterable_to_df([0, 1, -1], axis=1) == [1, 2, 3]


def test_convert_slice():
    df = pam.DataFrame(
        {
            "a": [0, 0, 0, 0, 0],
            "one": [0, 1, 2, 3, 4],
            "two": [1, 2, 3, 4, 5],
            "three": [2, 3, 4, 5],
            "b": [0, 0, 0, 0, 0],
        }
    )
    # try all on native dataframe
    assert df.convert_slice(slice(None, None), axis=1) == slice(0, 5)
    assert df.convert_slice(slice(1, None), axis=1) == slice(1, 5)
    assert df.convert_slice(slice(1, -1), axis=1) == slice(1, 4)
    assert df.convert_slice(slice(1, 2), axis=1) == slice(1, 2)

    assert df.convert_slice(slice(None, None), axis=0) == slice(0, 5)
    assert df.convert_slice(slice(1, None), axis=0) == slice(1, 5)
    assert df.convert_slice(slice(1, -1), axis=0) == slice(1, 4)
    assert df.convert_slice(slice(1, 2), axis=0) == slice(1, 2)

    df = df.iloc[1:-1, 1:-1]
    # df.view = (slice(1, 4), slice(1, 4))
    assert df.convert_slice(slice(None, None), axis=1) == slice(0, 3)
    assert df.convert_slice(slice(1, None), axis=1) == slice(1, 3)
    assert df.convert_slice(slice(1, -1), axis=1) == slice(1, 2)
    assert df.convert_slice(slice(1, 2), axis=1) == slice(1, 2)

    assert df.convert_slice(slice(None, None), axis=0) == slice(0, 3)
    assert df.convert_slice(slice(1, None), axis=0) == slice(1, 3)
    assert df.convert_slice(slice(1, -1), axis=0) == slice(1, 2)
    assert df.convert_slice(slice(1, 2), axis=0) == slice(1, 2)


def test_df_getitem():
    # tests for getting dataframes from non-view dataframes
    long_df = pam.DataFrame(
        {
            "a": [0, 0, 0, 0, 0],
            "one": [0, 0, 1, 2, 0],
            "two": [0, 1, 2, 3, 0],
            "three": [2, 3, 4, 5],
            "b": [0, 0, 0, 0, 0],
        },
        index=[10, 11, 12, 13, 14],
    )
    exp_columns = ("one", "two", "three")
    exp_index = (11, 12, 13)
    exp_values = [[0, 1, 3], [1, 2, 4], [2, 3, 5]]

    # non-view tests
    df1 = long_df.iloc[1:-1, 1:-1]
    assert df1.values == exp_values
    assert df1.columns == exp_columns
    assert df1.index == exp_index
    df2 = long_df.iloc[1:-1, [1, 2, 3]]
    assert df2.columns == exp_columns
    assert df2.values == exp_values
    assert df2.index == exp_index
    df3 = long_df.iloc[[1, 2, 3], 1:-1]
    assert df3.columns == exp_columns
    assert df3.values == exp_values
    assert df3.index == exp_index
    df4 = long_df.iloc[[1, 2, 3], [1, 2, 3]]
    assert df4.columns == exp_columns
    assert df4.values == exp_values
    assert df4.index == exp_index

    # view tests
    df = df1
    exp_columns = ("two", "three")
    exp_index = (12, 13)
    exp_values = [[2, 4], [3, 5]]
    df1 = df.iloc[1:, 1:]
    assert df1.values == exp_values
    assert df1.columns == exp_columns
    assert df1.index == exp_index
    df2 = df.iloc[1:, [1, 2]]
    assert df2.columns == exp_columns
    assert df2.values == exp_values
    assert df2.index == exp_index
    df3 = df.iloc[[1, 2], 1:]
    assert df3.columns == exp_columns
    assert df3.values == exp_values
    assert df3.index == exp_index
    df4 = df.iloc[[1, 2], [1, 2]]
    assert df4.columns == exp_columns
    assert df4.values == exp_values
    assert df4.index == exp_index

    # Tests for getting series from non-view dataframes
    ## Tests for dataframes without custom index
    ### test for view, int
    df = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    ser = df.iloc[1:, 0]
    assert ser.index == (1, 2)
    assert ser.values == [2, 3]
    assert all(ser == pam.Series([2, 3], index=[1, 2]))

    ## Tests for dtaframes with custom index
    # test dataframe iloc matches a series with a custom index from a column
    df = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]}, index=[10, 11, 12])
    ser = df.iloc[1:, 0]
    assert ser.index == (11, 12)
    assert ser.values == [2, 3]
    assert df.iloc[0, 1] == 2
    assert all(ser == pam.Series([2, 3], index=[11, 12]))

    # test dataframe iloc matches a series with a custom index from a column
    df = pam.DataFrame(
        {"one": [1, 2, 3], "two": [2, 3, 4], "three": [3, 4, 5], "four": [4, 5, 6]},
        index=[10, 11, 12],
    )
    ser = df.iloc[0, 0:2]
    assert ser.index == ("one", "two")
    assert ser.values == [1, 2]
    assert df.iloc[0, 1] == 2
    assert all(ser == pam.Series([1, 2], index=["one", "two"]))

    # test for view dataframes
    long_df = pam.DataFrame(
        {
            "a": [0, 0, 0, 0, 0],
            "one": [0, 0, 1, 2, 0],
            "two": [0, 1, 2, 3, 0],
            "three": [2, 3, 4, 5],
            "b": [0, 0, 0, 0, 0],
        }
    )

    df = long_df.iloc[1:-1, 1:-1]
    ser = df.iloc[1:, 0]
    assert ser.index == (2, 3)
    assert ser.values == [1, 2]
    assert ser.name == "one"
    assert df.iloc[0, 1] == 1
    assert all(ser == pam.Series([1, 2], index=[2, 3]))

    # test for view dataframe
    # non-custom index
    long_df = pam.DataFrame(
        {
            "a": [0, 0, 0, 0, 0],
            "one": [0, 0, 1, 2, 0],
            "two": [0, 1, 2, 3, 0],
            "three": [2, 3, 4, 5],
            "b": [0, 0, 0, 0, 0],
        }
    )

    df = long_df.iloc[1:-1, 1:-1]
    ser = df.iloc[1:, 0]
    assert ser.index == (2, 3)
    assert ser.values == [1, 2]
    assert ser.name == "one"
    assert df.iloc[0, 1] == 1
    assert all(ser == pam.Series([1, 2], index=[2, 3]))

    long_df = pam.DataFrame(
        {
            "a": [0, 0, 0, 0, 0],
            "one": [0, 0, 1, 2, 0],
            "two": [0, 1, 2, 3, 0],
            "three": [2, 3, 4, 5],
            "b": [0, 0, 0, 0, 0],
        },
        index=[10, 11, 12, 13, 14],
    )

    df = long_df.iloc[1:-1, 1:-1]
    ser = df.iloc[1:, 0]
    assert ser.index == (12, 13)
    assert ser.values == [1, 2]
    assert ser.name == "one"
    assert df.iloc[0, 1] == 1
    assert all(ser == pam.Series([1, 2], index=[12, 13]))


def test_df_setitem():

    # test non-view dataframe
    df = pam.DataFrame({"one": [0, 1, 2], "two": [1, 2, 3]})
    df.iloc[0, 0] = 99
    assert df.iloc[0, 0] == 99
    df.iloc[:, 0] = 100
    assert df.iloc[:, 0].values == [100, 100, 100]
    df.iloc[0, :] = 99
    assert df.iloc[0, :] == [99, 99]
    df.iloc[[0, 1, 2], 1] = 9
    assert df.iloc[0, :] == [9, 9, 9]

    # test on view dataframe
    df = pam.DataFrame(
        {
            "nil": [0, 0, 0, 0, 0],
            "one": [0, 0, 1, 2, 0],
            "two": [0, 1, 2, 3, 0],
            "three": [0, 0, 0, 0, 0],
        }
    )
    df1 = df.iloc[1:-1, 1:-1]
    df1.iloc[0, 0] = 99
    assert df1.iloc[0, 0] == 99
    df1.iloc[:, 0] = 100
    assert df1.iloc[:, 0].values == [100, 100, 100]
    df1.iloc[0, :] = 99
    assert df1.iloc[0, :] == [99, 99]
    df1.iloc[[0, 1, 2], 1] = 9
    assert df1.iloc[0, :] == [9, 9, 9]
