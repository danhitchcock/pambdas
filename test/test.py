import pam
import pandas


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


def test_df_iloc():

    # test dataframe iloc slice, int matches a series from a column
    df = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]})
    ser = df.iloc[1:, 0]
    assert ser.index == (1, 2)
    assert ser.values == [2, 3]
    assert df.iloc[0, 1] == 2
    assert ser == pam.Series([2, 3], index=[1, 2])

    # test dataframe iloc matches a series with a custom index from a column
    df = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]}, index=[10, 11, 12])
    ser = df.iloc[1:, 0]
    assert ser.index == (11, 12)
    assert ser.values == [2, 3]
    assert df.iloc[0, 1] == 2
    assert ser == pam.Series([2, 3], index=[11, 12])

    # test iloc between dataframes share data
    df = pam.DataFrame({"one": [1, 2, 3], "two": [2, 3, 4]}, index=[10, 11, 12])
