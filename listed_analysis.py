import pandas as pd
import glob

# ETL process: raw -> merge -> clean -> output

# 1. Load & merge data from CSV files 
files = glob.glob("data/raw/CRMLSListing*.csv")

df_list = []

for file in files:
    df = pd.read_csv(file)
    df_list.append(df)

if len(df_list) == 0:
    print(" No files found! Check your file path.")
else:
    all_data = pd.concat(df_list, ignore_index=True)

# 2. Cleaning data:
from pathlib import Path
import pandas as pd

INPUT_FILE = Path("output/listed_all1.csv")
OUTPUT_FILE = Path("output/cleaned_listed_all.csv")

NUMERIC_COLUMNS = [
    "OriginalListPrice",
    "ListPrice",
    "ClosePrice",
    "LivingArea",
    "LotSizeSquareFeet",
    "LotSizeArea",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "YearBuilt",
    "DaysOnMarket",
    "Latitude",
    "Longitude",
]

DATE_COLUMNS = [
    "CloseDate",
    "ContractStatusChangeDate",
    "PurchaseContractDate",
    "ListingContractDate",
]

STRING_COLUMNS = [
    "ListingId",
    "MlsStatus",
    "PropertyType",
    "PropertySubType",
    "City",
    "StateOrProvince",
    "PostalCode",
    "UnparsedAddress",
]

ACTIVE_STATUSES = ["Active", "Pending", "ActiveUnderContract", "ComingSoon"]


def merge_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    dup_cols = [c for c in df.columns if c.endswith(".1")]
    for dup_col in dup_cols:
        base_col = dup_col[:-2]
        if base_col in df.columns:
            df[base_col] = df[base_col].fillna(df[dup_col])
            df = df.drop(columns=[dup_col])
        else:
            df = df.rename(columns={dup_col: base_col})
    return df


def clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in STRING_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()
            df[col] = df[col].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return df


def clean_listed_data(df: pd.DataFrame) -> pd.DataFrame:
    df = merge_duplicate_columns(df)
    df.columns = df.columns.str.strip()

    df = clean_strings(df)

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "MlsStatus" in df.columns:
        df = df[df["MlsStatus"].isin(ACTIVE_STATUSES)]

    df = df.dropna(subset=["ListingId", "ListPrice", "PropertyType", "City"])

    df = df[(df["ListPrice"] >= 10000) & (df["ListPrice"] <= 100000000)]

    residential_mask = df["PropertyType"].eq("Residential")
    if "LivingArea" in df.columns:
        df = df[(~residential_mask) | (df["LivingArea"].fillna(0) >= 200)]

    if "BedroomsTotal" in df.columns:
        df = df[(df["BedroomsTotal"].isna()) | (df["BedroomsTotal"] >= 0)]

    if "BathroomsTotalInteger" in df.columns:
        df = df[(df["BathroomsTotalInteger"].isna()) | (df["BathroomsTotalInteger"] >= 0)]

    if "YearBuilt" in df.columns:
        df = df[(df["YearBuilt"].isna()) | ((df["YearBuilt"] >= 1800) & (df["YearBuilt"] <= 2030))]

    if "Latitude" in df.columns:
        df = df[(df["Latitude"].isna()) | ((df["Latitude"] >= -90) & (df["Latitude"] <= 90))]

    if "Longitude" in df.columns:
        df = df[(df["Longitude"].isna()) | ((df["Longitude"] >= -180) & (df["Longitude"] <= 180))]

    df = df.sort_values(
        by=["ContractStatusChangeDate", "ListingContractDate"],
        na_position="last"
    )

    df = df.drop_duplicates(subset=["ListingId"], keep="last")

    return df


def main():
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    cleaned_df = clean_listed_data(df)
    cleaned_df.to_csv(OUTPUT_FILE, index=False)

    print(f"Original shape: {df.shape}")
    print(f"Cleaned shape: {cleaned_df.shape}")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

    
