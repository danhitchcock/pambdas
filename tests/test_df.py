import datetime
from datetime import timezone
import pam
import pytest
from pam.other_stuff import nan
from functools import reduce


def test_is_2d_bool():
    a = [True]
    assert not pam.other_stuff.is_2d_bool(a)
    a = [[True, False, True, True], [False, True, False, False]]
    assert pam.other_stuff.is_2d_bool(a)
    a = pam.DataFrame({"one": [True, True], "two": [True, True]})
    assert pam.other_stuff.is_2d_bool(a)


def test_init_series():
    # test dictionary, default index
    ser = pam.Series([1, 100, 1000, 10000])
    assert ser.index == (0, 1, 2, 3)
    assert ser.values == [1, 100, 1000, 10000]

    ser = pam.Series([1, 100, 1000, 10000], index=[10, 11, 12, 13])
    assert ser.index == (10, 11, 12, 13)
    assert ser.values == [1, 100, 1000, 10000]


def test_invert():
    a = [True, False, True]
    assert pam.other_stuff.invert(a) == [False, True, False]
    assert a == [True, False, True]

    a = [[True, False, True], [False, False, True]]
    assert pam.other_stuff.invert(a) == [[False, True, False], [True, True, False]]
    assert a == [[True, False, True], [False, False, True]]


def test_series_methods():
    # drop
    a = pam.Series([0, 1, 2, 3])
    a.drop(2)

    # astype
    a = pam.DataFrame([[1, 2, 4], [2, 3, 4]])
    a.iloc[0] = a.iloc[0].astype(float)
    assert a.values == [[1.0, 2.0, 4.0], [2, 3, 4]]

    a = pam.DataFrame([[1, 2, 4], [2, 3, 4]])
    a.iloc[:, 0] = a.iloc[:, 0].astype(float)
    assert a.values == [[1.0, 2, 4], [2.0, 3, 4]]

    a = pam.DataFrame([[1, 2, 4], [2, 3, 4]])
    a.iloc[0] = a.iloc[0].apply(lambda x: x ** 2)
    assert a.values == [[1, 4, 16], [2, 3, 4]]

    a = pam.DataFrame([[1, 2, 4], [2, 3, 4]])
    a.iloc[:, 0] = a.iloc[:, 0].apply(lambda x: x ** 2)
    assert a.values == [[1, 2, 4], [4, 3, 4]]

    # sort_values
    ser = pam.Series([1, 10, 20, pam.other_stuff.nan, 40, 20, pam.other_stuff.nan])
    assert ser.sort_values().values == [
        1,
        10,
        20,
        20,
        40,
        pam.other_stuff.nan,
        pam.other_stuff.nan,
    ]
    assert ser.sort_values(na_position="first").values == [
        pam.other_stuff.nan,
        pam.other_stuff.nan,
        1,
        10,
        20,
        20,
        40,
    ]
    assert ser.sort_values(ascending=False).values == [
        40,
        20,
        20,
        10,
        1,
        pam.other_stuff.nan,
        pam.other_stuff.nan,
    ]
    assert ser.sort_values(ascending=False, na_position="first").values == [
        pam.other_stuff.nan,
        pam.other_stuff.nan,
        40,
        20,
        20,
        10,
        1,
    ]

    # Test string
    ser = pam.Series(["one", "two", "three"])
    assert ser.str.upper().values == ["ONE", "TWO", "THREE"]

    ser = pam.Series(
        [
            datetime.datetime(2007, 12, 6, 16, 29, 43),
            datetime.datetime(2007, 12, 6, 16, 29, 43),
            datetime.datetime(2007, 12, 6, 16, 29, 43),
        ]
    )

    # Test Datetime
    assert ser.dt.replace(tzinfo=timezone.utc).values == [
        datetime.datetime(2007, 12, 6, 16, 29, 43, tzinfo=datetime.timezone.utc),
        datetime.datetime(2007, 12, 6, 16, 29, 43, tzinfo=datetime.timezone.utc),
        datetime.datetime(2007, 12, 6, 16, 29, 43, tzinfo=datetime.timezone.utc),
    ]
    assert ser == pam.Series(
        [
            datetime.datetime(2007, 12, 6, 16, 29, 43),
            datetime.datetime(2007, 12, 6, 16, 29, 43),
            datetime.datetime(2007, 12, 6, 16, 29, 43),
        ]
    )


def test_df_read_csv():
    df = pam.read_csv("tests/test.csv")
    assert df.values == [
        ["0", "hello", "0", "10/1/2020"],
        ["1", "world", "1", "10/2/2020"],
        ["2", "how", "2", "10/3/2020"],
        ["hi", "are", "3", "10/4/2020"],
    ]
    assert df.index == (0, 1, 2, 3)
    assert df.columns == ("one", "two", "three", "four")

    df = pam.read_csv("tests/test.csv", index_col=1)
    assert df.values == [
        ["0", "0", "10/1/2020"],
        ["1", "1", "10/2/2020"],
        ["2", "2", "10/3/2020"],
        ["hi", "3", "10/4/2020"],
    ]
    assert df.columns == ("one", "three", "four")
    assert df.index == ("hello", "world", "how", "are")

    df = pam.read_csv("tests/test.csv", index_col=1, names=["a", "b", "c"])
    assert df.values == [
        ["one", "three", "four"],
        ["0", "0", "10/1/2020"],
        ["1", "1", "10/2/2020"],
        ["2", "2", "10/3/2020"],
        ["hi", "3", "10/4/2020"],
    ]
    assert df.columns == ("a", "b", "c")
    assert df.index == ("two", "hello", "world", "how", "are")


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
    assert df.bound_slice_to_df(slice(None, 1), axis=0) == slice(2, 3)
    assert df.bound_slice_to_df(slice(None, 100), axis=0) == slice(2, 4)
    assert df.bound_slice_to_df(slice(None, -1), axis=0) == slice(2, 3)
    assert df.bound_slice_to_df(slice(None, -100), axis=0) == slice(2, 2)

    assert df.bound_slice_to_df(slice(1, None), axis=0) == slice(3, 4)
    assert df.bound_slice_to_df(slice(1, 1), axis=0) == slice(3, 3)
    assert df.bound_slice_to_df(slice(1, 100), axis=0) == slice(3, 4)
    assert df.bound_slice_to_df(slice(1, -1), axis=0) == slice(3, 3)
    assert df.bound_slice_to_df(slice(1, -100), axis=0) == slice(3, 2)

    assert df.bound_slice_to_df(slice(100, None), axis=0) == slice(4, 4)
    assert df.bound_slice_to_df(slice(100, 1), axis=0) == slice(4, 3)
    assert df.bound_slice_to_df(slice(100, 100), axis=0) == slice(4, 4)
    assert df.bound_slice_to_df(slice(100, -1), axis=0) == slice(4, 3)
    assert df.bound_slice_to_df(slice(100, -100), axis=0) == slice(4, 2)

    assert df.bound_int_to_df(0, axis=0) == 2
    assert df.bound_int_to_df(-1, axis=0) == 3
    assert df.bound_iterable_to_df([0, 1, -1], axis=0) == [2, 3, 3]

    assert df.bound_slice_to_df(slice(None, None), axis=1) == slice(1, 4)
    assert df.bound_slice_to_df(slice(None, 1), axis=1) == slice(1, 2)
    assert df.bound_slice_to_df(slice(None, 100), axis=1) == slice(1, 4)
    assert df.bound_slice_to_df(slice(None, -1), axis=1) == slice(1, 3)
    assert df.bound_slice_to_df(slice(None, -100), axis=1) == slice(1, 1)

    assert df.bound_slice_to_df(slice(1, None), axis=1) == slice(2, 4)
    assert df.bound_slice_to_df(slice(1, 1), axis=1) == slice(2, 2)
    assert df.bound_slice_to_df(slice(1, 100), axis=1) == slice(2, 4)
    assert df.bound_slice_to_df(slice(1, -1), axis=1) == slice(2, 3)
    assert df.bound_slice_to_df(slice(1, -100), axis=1) == slice(2, 1)

    assert df.bound_slice_to_df(slice(100, None), axis=1) == slice(4, 4)
    assert df.bound_slice_to_df(slice(100, 1), axis=1) == slice(4, 2)
    assert df.bound_slice_to_df(slice(100, 100), axis=1) == slice(4, 4)
    assert df.bound_slice_to_df(slice(100, -1), axis=1) == slice(4, 3)
    assert df.bound_slice_to_df(slice(100, -100), axis=1) == slice(4, 1)

    assert df.bound_int_to_df(0, axis=1) == 1
    assert df.bound_int_to_df(-1, axis=1) == 3
    assert df.bound_iterable_to_df([0, 1, -1], axis=1) == [1, 2, 3]


def test_bound_ser():
    a = pam.Series([0, 1, 2, 3])

    assert a.bound_slice(slice(0, 3)) == slice(0, 3, 1)
    assert a.bound_slice(slice(1, 100)) == slice(1, 4, 1)
    assert a.bound_slice(slice(1, -1)) == slice(1, 3, 1)
    assert a.bound_slice(slice(-3, -1)) == slice(1, 3, 1)

    df = pam.DataFrame([[99, 98, 97, 97], [0, 1, 2, 3]])
    a = df.iloc[1, :]
    assert a.bound_slice(slice(0, 3)) == slice(1, 7, 2)
    assert a.bound_slice(slice(1, 100)) == slice(3, 9, 2)
    assert a.bound_slice(slice(1, -1)) == slice(3, 7, 2)
    assert a.bound_slice(slice(-3, -1)) == slice(3, 7, 2)

    a = df.iloc[:, 1]
    assert a.bound_slice(slice(None, None)) == slice(2, 4, 1)
    assert a.bound_slice(slice(None, 1)) == slice(2, 3, 1)
    assert a.bound_slice(slice(None, 100)) == slice(2, 4, 1)
    assert a.bound_slice(slice(None, -1)) == slice(2, 3, 1)
    assert a.bound_slice(slice(None, -100)) == slice(2, 2, 1)
    assert a.bound_slice(slice(1, None)) == slice(3, 4, 1)
    assert a.bound_slice(slice(100, None)) == slice(4, 4, 1)
    assert a.bound_slice(slice(-1, None)) == slice(3, 4, 1)
    assert a.bound_slice(slice(-100, None)) == slice(2, 4, 1)


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


def test_ser_getitem():
    # Test iloc

    ## test from series
    ser = pam.Series([0, 1, 2, 3, 4], index=["zero", "one", "two", "three", "four"])
    #### test integer
    assert ser.iloc[0] == 0
    assert ser.iloc[4] == 4
    with pytest.raises(IndexError):
        assert ser.iloc[6] == 100
    #### test slice
    assert ser.iloc[:].values == [0, 1, 2, 3, 4]
    assert ser.iloc[:3].values == [0, 1, 2]
    assert ser.iloc[:10].values == [0, 1, 2, 3, 4]

    #### test iterable
    assert ser.iloc[[0, 1, 2]].values == [0, 1, 2]

    #### test bool
    assert ser.iloc[[True, True, False, False, True]].values == [0, 1, 4]
    assert ser.iloc[pam.Series([True, True, False, False, True])].values == [0, 1, 4]

    # test from series view of a dataframe
    df = pam.DataFrame(
        [[11, 12, 13, 14, 15], [0, 1, 2, 3, 4]],
        columns=["zero", "one", "two", "three", "four"],
    )
    ser = df.iloc[1, :]
    #### test integer
    assert ser.iloc[0] == 0
    assert ser.iloc[4] == 4
    with pytest.raises(IndexError):
        assert ser.iloc[6] == 100
    #### test slice
    assert ser.iloc[:].values == [0, 1, 2, 3, 4]
    assert ser.iloc[:3].values == [0, 1, 2]
    assert ser.iloc[:10].values == [0, 1, 2, 3, 4]

    #### test iterable
    assert ser.iloc[[0, 1, 2]].values == [0, 1, 2]

    #### test bool
    assert ser.iloc[[True, True, False, False, True]].values == [0, 1, 4]
    assert ser.iloc[pam.Series([True, True, False, False, True])].values == [0, 1, 4]

    # test on a slice of series
    ser = pam.Series([0, 1, 2, 3, 4], index=["zero", "one", "two", "three", "four"])
    ser2 = ser.iloc[:-1]
    assert ser2.iloc[0:6].values == [0, 1, 2, 3]
    with pytest.raises(IndexError):
        assert ser2.iloc[5] == 4


def test_ser_setitem():
    # Test iloc

    ## test from series
    ser = pam.Series([0, 1, 2, 3, 4], index=["zero", "one", "two", "three", "four"])
    #### test integer
    ser.iloc[0] = 9
    assert ser.values == [9, 1, 2, 3, 4]
    ser.iloc[4] = 99
    assert ser.values == [9, 1, 2, 3, 99]
    with pytest.raises(IndexError):
        ser.iloc[6] = 100
    #### test slice

    ser.iloc[:] = [10, 11, 12, 13, 14]
    assert ser.values == [10, 11, 12, 13, 14]
    ser.iloc[:3] = 3
    assert ser.values == [3, 3, 3, 13, 14]
    ser.iloc[:5] = [99, 99, 99, 99, 99]
    assert ser.values == [99, 99, 99, 99, 99]

    #### test iterable
    ser.iloc[[0, 1, 3]] = 100
    assert ser.values == [100, 100, 99, 100, 99]

    #### test bool
    ser.iloc[[True, True, False, False, True]] = [0, 1, 4]
    assert ser.values == [0, 1, 99, 100, 4]
    ser.iloc[pam.Series([True, True, False, False, True])] = [10, 11, 14]
    assert ser.values == [10, 11, 99, 100, 14]

    # test from series view of a dataframe
    ## horizontal/row slice
    df = pam.DataFrame(
        [[11, 12, 13, 14, 15], [0, 1, 2, 3, 4]],
        columns=["zero", "one", "two", "three", "four"],
    )
    ser = df.iloc[1, :]
    #### test integer
    ser.iloc[0] = 9
    assert ser.values == [9, 1, 2, 3, 4]
    ser.iloc[4] = 99
    assert ser.values == [9, 1, 2, 3, 99]
    with pytest.raises(IndexError):
        ser.iloc[6] = 100
    #### test slice
    ser.iloc[:] = [10, 11, 12, 13, 14]
    assert ser.values == [10, 11, 12, 13, 14]
    ser.iloc[:3] = 3
    assert ser.values == [3, 3, 3, 13, 14]
    ser.iloc[:5] = [99, 99, 99, 99, 99]
    assert ser.values == [99, 99, 99, 99, 99]
    #### test iterable
    ser.iloc[[0, 1, 3]] = 100
    assert ser.values == [100, 100, 99, 100, 99]
    #### test bool
    ser.iloc[[True, True, False, False, True]] = [0, 1, 4]
    assert ser.values == [0, 1, 99, 100, 4]
    ser.iloc[pam.Series([True, True, False, False, True])] = [10, 11, 14]
    assert ser.values == [10, 11, 99, 100, 14]

    ## vertical/column slice
    df = pam.DataFrame(
        [[11, 12, 13, 14, 15], [0, 1, 2, 3, 4]],
        columns=["zero", "one", "two", "three", "four"],
    )
    ser = df.iloc[:, 1]
    #### test integer
    ser.iloc[0] = 9
    assert ser.values == [9, 1]
    with pytest.raises(IndexError):
        ser.iloc[6] = 100
    #### test slice
    ser.iloc[:] = [20, 30]
    assert ser.values == [20, 30]
    ser.iloc[:300] = 3
    assert ser.values == [3, 3]
    #### test iterable
    ser.iloc[[0, 1]] = 100
    assert ser.values == [100, 100]
    #### test bool
    ser.iloc[[True, False]] = [99]
    assert ser.values == [99, 100]

    # test on a slice of series
    ser = pam.Series([0, 1, 2, 3, 4], index=["zero", "one", "two", "three", "four"])
    ser2 = ser.iloc[:-1]
    with pytest.raises(IndexError):
        ser2.iloc[4] = 10


def test_df_methods():
    # test transpose
    df = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    df2 = df.transpose()
    assert df.equals(pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]}))
    assert df.columns == ("one", "two")
    assert df.index == (0, 1, 2)
    assert df2.values == [[1, 2, 3], [2, 3, 4]]
    assert df2.index == ("one", "two")
    assert df2.columns == (0, 1, 2)

    # test applymap
    df = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    assert df.applymap(lambda x: x ** 2).values == [[1, 4], [4, 9], [9, 16]]
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    # test apply
    ## non-reducing
    ### rows
    df = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    assert df.apply(lambda x: x ** 2, axis=0).values == [[1, 4], [4, 9], [9, 16]]
    assert df.values == [[1, 2], [2, 3], [3, 4]]
    ### columns
    assert df.apply(lambda x: x ** 2, axis=1).values == [[1, 4], [4, 9], [9, 16]]
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    ## reducing

    def multiply(iterable):
        res = reduce((lambda x, y: x * y), iterable)
        return res

    ### rows
    assert df.apply(multiply, axis=0).values == [2, 6, 12]
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    ### columns
    assert df.apply(multiply, axis=1).values == [6, 24]
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    ### rows
    assert df.apply(sum, axis=0).values == [3, 5, 7]
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    ### columns
    assert df.apply(sum, axis=1).values == [6, 9]
    assert df.values == [[1, 2], [2, 3], [3, 4]]

    # sort values
    df = pam.DataFrame({"one": [1, 2, nan, 0, -1], "two": [nan, 10, nan, 0, -1]})
    assert df.sort_values(by="one", axis=0).values == [
        [-1, -1],
        [0, 0],
        [1, nan],
        [2, 10],
        [nan, nan],
    ]
    assert df.sort_values(by="one", na_position="first", axis=0).values == [
        [nan, nan],
        [-1, -1],
        [0, 0],
        [1, nan],
        [2, 10],
    ]
    assert df.sort_values(by="one", ascending=False, axis=0).values == [
        [2, 10],
        [1, nan],
        [0, 0],
        [-1, -1],
        [nan, nan],
    ]

    assert df.sort_values(
        by="one", ascending=False, na_position="first", axis=0
    ).values == [[nan, nan], [2, 10], [1, nan], [0, 0], [-1, -1]]

    assert df.sort_values(
        by="two", ascending=False, na_position="first", axis=0
    ).values == [[1, nan], [nan, nan], [2, 10], [0, 0], [-1, -1]]
    assert df.equals(
        pam.DataFrame({"one": [1, 2, nan, 0, -1], "two": [nan, 10, nan, 0, -1]})
    )

    df = pam.DataFrame({"one": [1, 2, nan, 0, 10], "two": [nan, 10, nan, 0, -1]})
    assert df.sort_values(by=0, axis=1).values == [
        [1, nan],
        [2, 10],
        [nan, nan],
        [0, 0],
        [10, -1],
    ]
    assert df.sort_values(by=4, axis=1).values == [
        [nan, 1],
        [10, 2],
        [nan, nan],
        [0, 0],
        [-1, 10],
    ]
    assert df.equals(
        pam.DataFrame({"one": [1, 2, nan, 0, 10], "two": [nan, 10, nan, 0, -1]})
    )

    # test reset_index
    df = pam.DataFrame(
        {"one": [1, 2, nan, 0, 10], "two": [nan, 10, nan, 0, -1]},
        index=[10, 11, 12, 13, 14],
    )
    ## test without drop
    df2 = df.reset_index()
    assert (
        df.values
        == pam.DataFrame(
            {"one": [1, 2, nan, 0, 10], "two": [nan, 10, nan, 0, -1]},
            index=[10, 11, 12, 13, 14],
        ).values
    )
    assert df2.values == [
        [10, 1, nan],
        [11, 2, 10],
        [12, nan, nan],
        [13, 0, 0],
        [14, 10, -1],
    ]
    assert df2.index == (0, 1, 2, 3, 4)

    ## test with drop
    df2 = df.reset_index(drop=True)
    assert (
        df.values
        == pam.DataFrame(
            {"one": [1, 2, nan, 0, 10], "two": [nan, 10, nan, 0, -1]},
            index=[10, 11, 12, 13, 14],
        ).values
    )

    assert df.equals(df2)
    assert df2.index == (0, 1, 2, 3, 4)


def test_df_operators():
    df = pam.DataFrame([[0, 10, 20], [1, 11, 21]])
    assert (df > 10).values == [[False, False, True], [False, True, True]]
    assert (~(df > 10)).values == [[True, True, False], [True, False, False]]

    df1 = pam.DataFrame([[0, nan, 20], [1, 11, 21]])
    df2 = pam.DataFrame([[0, nan, 20], [1, 11, 21]])
    assert df1.equals(df2)
    assert df2.equals(df1)
    df2 = pam.DataFrame(
        [[0, 0, 0, 0, 0], [0, 0, nan, 20, 0], [0, 1, 11, 21, 0], [0, 0, 0, 0, 0]]
    )
    assert not df1.equals(df2)
    assert not df2.equals(df1)

    df2 = df2.iloc[1:-1, 1:-1]
    assert df1.equals(df2)
    assert df2.equals(df1)


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

    # Test 2d Boolean arrays
    # Test Boolean setitem
    # DataFrame
    exp_results = [[nan, nan, 100, 1000], [nan, nan, 200, 2000], [nan, 30, 300, 3000]]
    input = [[1, 10, 100, 1000], [2, 20, 200, 2000], [3, 30, 300, 3000]]
    df = pam.DataFrame(input)
    df = df[df > 20]
    assert df.values == exp_results

    df = pam.DataFrame(input)
    df = df.loc[df > 20]
    assert df.values == exp_results

    df = pam.DataFrame(input)
    df = df.iloc[df > 20]
    assert df.values == exp_results

    # 2d boolean array
    df = pam.DataFrame(input)
    df = df[(df > 20).values]
    assert df.values == exp_results

    df = pam.DataFrame(input)
    df = df.loc[(df > 20).values]
    assert df.values == exp_results

    df = pam.DataFrame(input)
    df = df.iloc[(df > 20).values]
    assert df.values == exp_results


def test_df_setitem():

    # test non-view dataframe
    df = pam.DataFrame({"one": [0, 1, 2], "two": [1, 2, 3]})
    df.iloc[0, 0] = 99
    assert df.iloc[0, 0] == 99
    df.iloc[:, 0] = 100
    assert df.iloc[:, 0].values == [100, 100, 100]
    df.iloc[0, :] = 99
    assert df.iloc[0, :].values == [99, 99]
    df.iloc[[0, 1, 2], 1] = 9
    assert df.iloc[0, :].values == [99, 9]
    df.iloc[0:2, 0:2] = [[10, 11], [10, 11]]
    assert df.iloc[0:2, 0:2].values == [[10, 11], [10, 11]]

    # test single item assignments to multiple cells by slices
    df = pam.DataFrame(
        {"one": [0, 0, 0, 0], "two": [0, 0, 0, 0], "three": [0, 0, 0, 0]}
    )
    df.iloc[1:3, 1:3] = 2
    assert df.values == [[0, 0, 0], [0, 2, 2], [0, 2, 2], [0, 0, 0]]
    df = pam.DataFrame(
        {"one": [0, 0, 0, 0], "two": [0, 0, 0, 0], "three": [0, 0, 0, 0]}
    )
    df.iloc[1:6, 1:6] = 2
    assert df.values == [[0, 0, 0], [0, 2, 2], [0, 2, 2], [0, 2, 2]]

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
    df1.iloc[1:50, 1:50] = 999
    assert df.values == [
        [0, 0, 0, 0],
        [0, 99, 9, 0],
        [0, 100, 999, 0],
        [0, 100, 999, 0],
        [0, 0, 0, 0],
    ]

    # Test Boolean setitem
    # DataFrame
    df = pam.DataFrame([[1, 10, 100, 1000], [2, 20, 200, 2000], [3, 30, 300, 3000]])
    df[df > 20] = 99
    assert df.values == [[1, 10, 99, 99], [2, 20, 99, 99], [3, 99, 99, 99]]

    df = pam.DataFrame([[1, 10, 100, 1000], [2, 20, 200, 2000], [3, 30, 300, 3000]])
    df.loc[df > 20] = 99
    assert df.values == [[1, 10, 99, 99], [2, 20, 99, 99], [3, 99, 99, 99]]

    df = pam.DataFrame([[1, 10, 100, 1000], [2, 20, 200, 2000], [3, 30, 300, 3000]])
    df.iloc[df > 20] = 99
    assert df.values == [[1, 10, 99, 99], [2, 20, 99, 99], [3, 99, 99, 99]]

    # 2d boolean array
    df = pam.DataFrame([[1, 10, 100, 1000], [2, 20, 200, 2000], [3, 30, 300, 3000]])
    df[(df > 20).values] = 99
    assert df.values == [[1, 10, 99, 99], [2, 20, 99, 99], [3, 99, 99, 99]]

    df = pam.DataFrame([[1, 10, 100, 1000], [2, 20, 200, 2000], [3, 30, 300, 3000]])
    df.loc[(df > 20).values] = 99
    assert df.values == [[1, 10, 99, 99], [2, 20, 99, 99], [3, 99, 99, 99]]

    df = pam.DataFrame([[1, 10, 100, 1000], [2, 20, 200, 2000], [3, 30, 300, 3000]])
    df.iloc[(df > 20).values] = 99
    assert df.values == [[1, 10, 99, 99], [2, 20, 99, 99], [3, 99, 99, 99]]


def test_df_merging():
    a = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    b = pam.DataFrame({"one": [10, 20, 30], "two": [20, 30, 40]})
    assert a.append(b).values == [[1, 2], [2, 3], [3, 4], [10, 20], [20, 30], [30, 40]]

    a = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    b = pam.DataFrame({"one": [10, 20, 30], "two": [20, 30, 40], "three": [0, 1, 1]})
    assert a.append(b).values == [
        [1, 2, nan],
        [2, 3, nan],
        [3, 4, nan],
        [10, 20, 0],
        [20, 30, 1],
        [30, 40, 1],
    ]
    assert b.append(a).values == [
        [10, 20, 0],
        [20, 30, 1],
        [30, 40, 1],
        [1, 2, nan],
        [2, 3, nan],
        [3, 4, nan],
    ]

    assert a.append(b, ignore_index=False).index == (0, 1, 2, 0, 1, 2)


def test_df_setitem_create():
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
    df.loc[:, "dne"] = [99, 99, 99]
    df.loc["dne", :] = 100


def test_ser_setitem_create():
    ser = pam.Series([0, 1, 2, 3, 4])
    ser.loc["dne"] = 100
