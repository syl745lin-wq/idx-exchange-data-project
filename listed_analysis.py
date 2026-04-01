import pandas as pd
import glob

files = glob.glob("data/raw/CRMLSListing*.csv")

df_list = []

for file in files:
    df = pd.read_csv(file)
    df_list.append(df)

if len(df_list) == 0:
    print(" No files found! Check your file path.")
else:
    all_data = pd.concat(df_list, ignore_index=True)

    print(all_data.shape)

    all_data.to_csv("output/listed_all.csv", index=False)
