from pam import DataFrame, Series

df = DataFrame(
    {"one": [1, 2, 3], "two": [4, 5, 6], "three": [7, 8, 9]}, index=[10, 20, 30]
)
# a = pd.Series({'one': [1, 2, 3, 4]})
df2 = df.iloc[:, 1:3]
# print("the view", df2.view)
df2["four"] = [99, 100, 101]
# print("the view", df2.view)

df2.iloc[0, 0] = 9999999
# print(df)
# print(df2)

print(df)
print(df.iloc[[1, 2], :])
print(df.iloc[:, [1, 2]])
print(df.iloc[[0, 2], [0, 2]])
print(df.iloc[[True, False, True], [True, False, True]])
