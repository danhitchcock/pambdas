import pandas as pd
import pam
import time

# one = list(range(10))
# iterations = 100
# mydict = {
#     "a": [1, 100, 1000, 10000],
#     "b": [2, 200, 2000, 20000],
#     "c": [3, 300, 3000, 30000],
#     "d": [4, 400, 4000, 30000],
# }
# df1 = pam.DataFrame(mydict, index=[10, 11, 12, 13])
# print(df1.iloc[:, 1:].index)
ser = pam.Series([1, 100, 1000, 10000])
ser2 = ser[:]
ser3 = ser.iloc[[0, 2]]
print(ser.iloc[0])
ser.iloc[0] = 99
print(ser.iloc[0])
ser.iloc[0:3] = [999, 888, 777]
print(ser.iloc[0:3])
ser.iloc[[0, 1]] = [222, 333]
print(ser.iloc[[0, 1]])
print(ser)
print(ser2)
print(ser3)
# print(df1)
# print(df1.values)
# df1 = pam.DataFrame(mydict)
# print(df1)
# print(df1.values)

#
# mydict = [{"a": [1, 2, 3]}, {"b": [3, 4, 5]}]
# df1 = pam.DataFrame(mydict)
# print(df1)
# mydict = [pam.Series([0, 1, 2, 3]), pam.Series([6, 5, 4, 3])]
# df1 = pam.DataFrame(mydict)
# print(df1)
# mydict = [pd.Series([0, 1, 2, 3]), pd.Series([6, 5, 4, 3])]
# df1 = pd.DataFrame(mydict)
# print(df1)
# a = df1["a"] < 200
# print(a)
# print("one")
# df1.iloc[0, 1] = 99
# print(df1)
# df1.iloc[:, 0] = pd.Series([888, 888, 777, 45])
# print(df1)
# df1.iloc[0:2, [0, 1, 2]] = [[889, 889, 779], [990, 990, 770]]
# print(df1)
# # df1.loc[[0, 1], "a":"d"] = [[8, 9, 10], [10, 11, 12]]
# print(df1)

# todo add a 'is_view' method
# todo [:,:] returns just the object itself
#
#
# ser = df1["two"]
# df2 = df1.iloc[:, 1:3]
# df2.iloc[0, 0] = 9999
# print("Shared Changes")
# print(df1)
# print(df2)
# print(ser)
#
# print("*" * 80)
# df1 = DataFrame(
#     {"one": [1, 2, 3], "two": [4, 5, 6], "three": [7, 8, 9]}, index=[10, 20, 30]
# )
# ser = df1["two"]
# df2 = df1.iloc[[0, 1, 2], 1:3]
# df2.iloc[0, 0] = 9999
# print("Not Shared Changes")
# print(df1)
# print(df2)
# print(ser)
# print("*" * 80)
# df1 = DataFrame(
#     {"one": [1, 2, 3], "two": [4, 5, 6], "three": [7, 8, 9]}, index=[10, 20, 30]
# )
# ser = df1["two"]
# df2 = df1.iloc[:, 1:3]
# df1["four"] = [10, 10, 10]
# df2.iloc[0, 0] = 9999
# print("Shared Changes")
# print(df1)
# print(df2)
# print(ser)
# print("*" * 80)
#
# df1 = DataFrame(
#     {"one": [1, 2, 3], "two": [4, 5, 6], "three": [7, 8, 9]}, index=[10, 20, 30]
# )
# ser = df1["two"]
# df2 = df1.iloc[:, 1:3]
# df2["four"] = [10, 10, 10]
# df2.iloc[0, 0] = 9999
# print("Not Shared Changes")
# print(df1)
# print(df2)
# print(ser)
# print("*" * 80)
