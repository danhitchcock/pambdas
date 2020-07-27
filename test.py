import pandas as pd
import pam
import time

one = list(range(4))
iterations = 1

t0 = time.time()
df1 = pam.DataFrame({"one": one, "two": one, "three": one}, index=["a", "b", "c", "d"])
for i in range(iterations):
    df2 = df1.iloc[1:4, :]

print(time.time() - t0)
print(df1.loc[["a", "c", "d"], "one":"three"])

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
