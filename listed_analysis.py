import pandas as pd
import glob

files = sorted(glob.glob("data/raw/CRMLSListing*.csv"))

print("amount of files:", len(files))

df = pd.concat([pd.read_csv(f) for f in files])

print("data shape:", df.shape)

df.to_csv("data/processed/listed_combined.csv", index=False)

print("finish listed data combination")
