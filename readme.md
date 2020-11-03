# Pandas-like DataFrames for Lambdas

A dependency-free pure Python dataframe closely matching the Pandas API, and all under 1 MB.

`pip install pambdas`

Pambdas is designed to replicate the behavior of a Pandas DataFrame, but not rely on large libraries or compiled C code. This need was born out of frustratingly packaging Pandas on AWS Lambda to do some very basic calculations. As such, it is much slower since it relies on the Python list as it's backend type.

Data is stored as a flat list, which much like Pandas, can be shared between DataFrames and Series.
For example, changes to `ser` and `df2` are reflected in `df1`.
```python
import pambdas as pam

df1 = pam.DataFrame(
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

Pambdas is in early beta, and by no means feature complete and certainly chock-full of bugs.

Currently, it supports:
* Indexing/assignment by .loc and .iloc
* Unary operators for Series
* Boolean Indexing
* Apply and ApplyMap
* Append
* GroupBy
* pambdas.concat
* pambdas.read_csv

Check out `example.py` and give it a shot!

## Contributing

* Every pull request must be associated with an issue.
* Every new feature should have a test.
* The code is formatted with Black
* Every branch name should start with the issue # it is addressing (i.e. 9_fix_concat)
* Every pull request should have detailed instructions on what the changes are, and how to test them.
