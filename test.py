import pam

# import pandas as pam

long_df = pam.DataFrame(
    {"one": [1, 7, 20], "two": [2, 0, 30], "three": [6, 5, 4]}, index=[10, 20, 30]
)
a = long_df["one"]
a.loc[0] = "one2"
# print(long_df["one"] < 5)
print(long_df[[True, False, False]])
# print(long_df["one"] < 5)
# print(long_df[long_df["one"] < 5])

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
