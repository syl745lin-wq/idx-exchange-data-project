import pandas as pd
import glob

files = glob.glob("data/raw/CRMLSSold*.csv")

df = pd.concat([pd.read_csv(f) for f in files])

print(df.shape)
