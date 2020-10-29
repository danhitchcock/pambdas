import pam

import pandas as pam

# ser = pam.Series([0, 1, 2, 3, 4], index=["zero", "two", "three", "four"])
# print(ser.iloc[[True, True, False, False, True]].values)

df = pam.DataFrame({"one": [0, 1, 2], "two": [1, 2, 3]})
df.iloc[0:2, 0:2] = [[13, 11], [10, 11]]
print(df.iloc[0:2, 0:2].values)
print(df)
# a = ser.iloc[1:-1]
# print(a.view)
# print(ser.view)
# a = long_df["one"]
# a.loc[0] = "one2"
# print(long_df)
# print(a.data)
# print(a.iloc[0:10].values)
# # print(long_df["one"] < 5)
# # print(long_df[[True, False, False]])
#
# # long_df[[True, False, False]] = 199
# # print(long_df)
# # print(long_df["one"] < 5)
# # long_df["one"][long_df["one"] < 5] = 199
# long_df.loc[[True, False, True], ["one", "three"]] = [[101, 102], [103, 104]]
# print(long_df)

# ong_df[long_df < 5]
# print(a)
# print(long_df)
# # a.loc[10:30] = 100
# print(a.loc[10:20])
# print(a)
# a[[10, 20]] = 9
# print(a)
# # a.loc[10:30] = 99
# b = a[:30]
# print(a, b)
# a.iloc[0] = 100
# print(a, b)
# # print("df info", long_df.shape, long_df.step, long_df.view, len(long_df.data))
# long_df.loc["a", :] = [1, 2]
# long_df.loc[:, "super"] = 39
# long_df.iloc[:] = 999
# print(long_df.iloc[0, :])
# print("*" * 80)
# print(long_df)
# long_df.__str__()
# long_df.add_empty_series("dne", axis=0)
# long_df.add_empty_series("dne", axis=1)
# print(long_df)
# df = long_df.iloc[1:-1, 1:-1]
#
# print(df)
# print(long_df)
# df.loc[:, "dne"] = [99, 99, 99]
# print(df)
# df.loc["dne", 0:2] = 199
# print(df)
