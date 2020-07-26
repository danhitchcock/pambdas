# Pandas for Lambdas
### My Attempt at a Pure Python version of Lambda

`git clone https://github.com/danhitchcock/pambda.git`  

`pip install -e pam`  

`python pambda/test.py`  


Pambda is designed to replicated the behavior of a Pandas DataFrame, but not rely on large libraries or compiled C code.

Data is stored as a flat list, and various series/dataframes have their own view of shared data.

The specific behavior I'm referring to is how views of a DataFrame are related. Take for example

```python
from pam import DataFrame

df1 = DataFrame(
    {"one": [1, 2, 3], "two": [4, 5, 6], "three": [7, 8, 9]}, index=[10, 20, 30]
)
ser = df1["two"]
df2 = df1.iloc[:, 1:3]
df2.iloc[0, 0] = 9999
print("Shared Changes")
print(df1)
print(df2)
print(ser)

```

Here, changes to `df2` are reflected in `df1`, and vice-versa, since the data are shared and viewed differently by slicing.
However, if we reference by a list instead:
```python
from pam import DataFrame

df1 = DataFrame(
    {"one": [1, 2, 3], "two": [4, 5, 6], "three": [7, 8, 9]}, index=[10, 20, 30]
)
ser = df1["two"]
df2 = df1.iloc[[0, 1, 2], 1:3]
df2.iloc[0, 0] = 9999
print("Not Shared Changes")
print(df1)
print(df2)
print(ser)
```
df2 creates a copy of the data, and these changes are not reflected.

In a similar situation, we can add a column to the *whole* dataset
```python
from pam import DataFrame

df1 = DataFrame(
    {"one": [1, 2, 3], "two": [4, 5, 6], "three": [7, 8, 9]}, index=[10, 20, 30]
)
ser = df1["two"]
df2 = df1.iloc[:, 1:3]
df1['four'] = [10, 10, 10]
df2.iloc[0, 0] = 9999
print("Shared Changes")
print(df1)
print(df2)
print(ser)
```

and changes are shared between all.
But, if we add that column to df2:
```python
from pam import DataFrame

df1 = DataFrame(
    {"one": [1, 2, 3], "two": [4, 5, 6], "three": [7, 8, 9]}, index=[10, 20, 30]
)
ser = df1["two"]
df2 = df1.iloc[:, 1:3]
df2['four'] = [10, 10, 10]
df2.iloc[0, 0] = 9999
print("Not Shared Changes")
print(df1)
print(df2)
print(ser)
```

The data is no longer shared.

It doesn't do a whole lot yet, but most of iloc is working for DataFrame, including boolean lookups