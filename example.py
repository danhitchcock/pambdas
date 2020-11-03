import datetime
import pam

print("Create DataFrame")
df = pam.DataFrame([[1, "a", 2.0], [2, "b", 3.0]], columns=["int", "char", "float"])
df = pam.DataFrame({"int": [1, 2], "char": ["a", "b"], "float": [1.0, 2.0]})
print(df)
print("*" * 80)

print("Assign values or apply")
df["int"] = df["int"].apply(lambda x: x * 10)
print(df)
print("*" * 80)

print("Access and set items with loc and iloc")
ser = df["int"]
ser = df.loc[:, "int"]
ser = df.iloc[:, 0]
print(ser)
print("*" * 80)

print("Setting an item on a slice will change the DataFrame")
ser.iloc[0] = 1000
print(df)
print("*" * 80)

print("Append a new series, or do unary operations by Python's rules")
df["result"] = df["int"] + df["float"]
print(df)
print("*" * 80)

print("Access by boolean")
print(df.loc[df["int"] <= 20, :])
print("*" * 80)

print("Modify strings with any string method, args")
df["char"] = df["char"].str.upper()
print(df)
print("*" * 80)

print("Even datetimes")
df["time"] = [datetime.datetime(2020, 5, 17), datetime.datetime(2020, 1, 1)]
df["time"] = df["time"].dt.astimezone(tz=None)
print(df["time"])
print("*" * 80)

print("Concatenate two DataFrames")
df2 = pam.DataFrame({"int": [99, 99], "new_col": ["new", "stuff"], "char": ["d", "e"]})
print(pam.concat([df, df2]))
print("*" * 80)

print("Groupby!")
df = pam.DataFrame(
    {
        "Time in shower": [15, 20, 21, 9, 15, 12],
        "Day of week": [
            "Sunday",
            "Saturday",
            "Saturday",
            "Sunday",
            "Saturday",
            "Sunday",
        ],
    }
)
print(df.groupby("Day of week").apply(lambda x: sum(x) / len(x)))
print("*" * 80)

print("And read a csv")
df = pam.read_csv("tests/test.csv")
print(df)
