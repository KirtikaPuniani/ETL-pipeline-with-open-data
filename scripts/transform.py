# import pandas as pd
# from extract import df

# df = df.dropna(subset=["Date_reported", "Country_code", "New_cases", "New_deaths"]).copy()

# df["Date_reported"] = pd.to_datetime(df["Date_reported"])

# df["New_deaths"] = df["New_deaths"].fillna(0)
# df["death_rate"] = df["New_deaths"] / df["New_cases"]

# df["death_rate"] = df["death_rate"].replace([float("inf"), -float("inf")], 0)

# df["year"] = df["Date_reported"].dt.year
# df["month"] = df["Date_reported"].dt.month
# df["day"] = df["Date_reported"].dt.day

import pandas as pd
from extract import df

# drop rows with missing important values and create a copy
df = df.dropna(subset=["Date_reported", "Country_code"])

# convert to datetime BEFORE using .dt
df["Date_reported"] = pd.to_datetime(df["Date_reported"], errors="coerce")

# remove rows where conversion failed
df = df.dropna(subset=["Date_reported"])

# fill missing numeric values
df["New_cases"] = df["New_cases"].fillna(0)
df["New_deaths"] = df["New_deaths"].fillna(0)

# create metric
df["death_rate"] = df["New_deaths"] / df["New_cases"]

# handle division by zero
df["death_rate"] = df["death_rate"].replace([float("inf"), -float("inf")], 0)

# create partition columns
df["year"] = df["Date_reported"].dt.year
df["month"] = df["Date_reported"].dt.month
df["day"] = df["Date_reported"].dt.day

print(df.head())