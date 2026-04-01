import pandas as pd
import glob
import os

os.makedirs("data/processed", exist_ok=True)

files = sorted(glob.glob("data/raw/CRMLSSold*.csv"))

print("sold document amount:", len(files))

if not files:
    raise FileNotFoundError("cannot find CRMLSSold CSV document")

df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)

print("data shape:", df.shape)

df.to_csv("data/processed/sold_combined.csv", index=False)

print("finish sold data combination")
