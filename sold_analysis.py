import pandas as pd
import glob

# ETL process: raw -> merge -> clean -> output

# 1. Load & merge data from CSV files：
files = glob.glob("data/raw/CRMLSSold*.csv")

df_list = []

for file in files:
    df = pd.read_csv(file)
    df_list.append(df)

if len(df_list) == 0:
    print(" No files found! Check your file path.")
else:
    all_data = pd.concat(df_list, ignore_index=True)

# 2. Cleaning data:

df = pd.read_csv("output/sold_all1.csv")

# change data type:
df[""] = pd.to_numeric(df[""], errors="coerce")
df[""] = pd.to_numeric(df[""], errors="coerce")
df[""] = pd.to_datetime(df[""], errors="coerce")

# Delete key missing values:
df = df.dropna(subset=["", ""])

# Remove outliers：
df = df[(df[""] > )]


# drop duplicates:
df = df.drop_duplicates()



#4. Output:

all_data.to_csv("output/cleaned_sold_all.csv", index=False)

print(all_data.shape)
