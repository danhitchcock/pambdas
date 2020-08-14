import pam as pam

long_df = pam.DataFrame({"one": [1, 2], "two": [2, 3]})


# long_df.loc[:, "dne"] = 99
# # print("df info", long_df.shape, long_df.step, long_df.view, len(long_df.data))
# long_df.loc["a", :] = [1, 2]
# long_df.loc[:, "super"] = 39
long_df[:, ["one", "two"]] = 100
print(long_df)
long_df.__str__()
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
