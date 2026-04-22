from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from week23_common import BASE_DIR, ensure_dir

INPUT_DIR = BASE_DIR / "output" / "week2_3" / "mortgage_enrichment"
OUTPUT_DIR = BASE_DIR / "output" / "week4_5"

DATE_COLUMNS = [
    "CloseDate",
    "PurchaseContractDate",
    "ListingContractDate",
    "ContractStatusChangeDate",
]

NUMERIC_COLUMNS = [
    "ClosePrice",
    "ListPrice",
    "OriginalListPrice",
    "LivingArea",
    "LotSizeAcres",
    "LotSizeSquareFeet",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "DaysOnMarket",
    "YearBuilt",
    "Latitude",
    "Longitude",
    "mortgage30us_monthly_avg",
]

COMMON_DROP_COLUMNS = [
    "ListAgentEmail",
    "BuyerAgentFirstName",
    "BuyerAgentLastName",
    "BuyerAgentMlsId",
    "BuyerOfficeName",
    "BuyerOfficeAOR",
    "ListAgentFirstName",
    "ListAgentLastName",
    "ListAgentFullName",
    "ListOfficeName",
    "CoListAgentFirstName",
    "CoListAgentLastName",
    "CoListOfficeName",
    "ListingKey",
    "ListingKeyNumeric",
    "ElementarySchool",
    "MiddleOrJuniorSchool",
    "HighSchool",
    "HighSchoolDistrict",
    "MLSAreaMajor",
    "SubdivisionName",
    "StreetNumberNumeric",
    "AssociationFeeFrequency",
    "AssociationFee",
    "MainLevelBedrooms",
    "AttachedGarageYN",
    "ParkingTotal",
    "Stories",
    "Levels",
    "GarageSpaces",
    "PoolPrivateYN",
    "NewConstructionYN",
    "FireplaceYN",
    "BuildingAreaTotal",
    "LotSizeArea",
]

SOLD_ONLY_DROP_COLUMNS = [
    "OriginatingSystemName",
    "OriginatingSystemSubName",
    "BuyerAgentAOR",
    "ListAgentAOR",
    "Flooring",
    "ViewYN",
    "latfilled",
    "lonfilled",
]

LISTED_ONLY_DROP_COLUMNS = [
    "BuyerAgencyCompensation",
    "BuyerAgencyCompensationType",
]


def dataset_paths(dataset_name: str) -> Dict[str, Path]:
    output_root = OUTPUT_DIR / dataset_name
    ensure_dir(output_root)
    return {
        "input": INPUT_DIR / f"{dataset_name}_residential_with_mortgage_rates.csv",
        "output_root": output_root,
        "cleaned": output_root / f"{dataset_name}_analysis_ready_week45.csv",
        "row_counts": output_root / f"{dataset_name}_week45_row_counts.csv",
        "transformation_log": output_root / f"{dataset_name}_transformation_log.csv",
        "dtypes": output_root / f"{dataset_name}_week45_dtypes.csv",
        "date_flags": output_root / f"{dataset_name}_date_flag_summary.csv",
        "geo_summary": output_root / f"{dataset_name}_geographic_quality_summary.csv",
        "missing_summary": output_root / f"{dataset_name}_missing_summary.csv",
        "invalid_numeric_summary": output_root / f"{dataset_name}_invalid_numeric_summary.csv",
    }


def load_dataset(dataset_name: str) -> pd.DataFrame:
    paths = dataset_paths(dataset_name)
    return pd.read_csv(paths["input"], low_memory=False)


def clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    object_columns = df.select_dtypes(include=["object"]).columns
    for column in object_columns:
        df[column] = df[column].astype("string").str.strip()
        df[column] = df[column].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return df


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    for column in DATE_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")

    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def make_date_flags(df: pd.DataFrame) -> pd.DataFrame:
    df["listing_after_close_flag"] = (
        df["ListingContractDate"].notna()
        & df["CloseDate"].notna()
        & (df["ListingContractDate"] > df["CloseDate"])
    )
    df["purchase_after_close_flag"] = (
        df["PurchaseContractDate"].notna()
        & df["CloseDate"].notna()
        & (df["PurchaseContractDate"] > df["CloseDate"])
    )
    df["negative_timeline_flag"] = (
        df["PurchaseContractDate"].notna()
        & df["ListingContractDate"].notna()
        & (df["PurchaseContractDate"] < df["ListingContractDate"])
    )
    return df


def make_geographic_flags(df: pd.DataFrame) -> pd.DataFrame:
    latitude = pd.to_numeric(df["Latitude"], errors="coerce")
    longitude = pd.to_numeric(df["Longitude"], errors="coerce")
    state = df["StateOrProvince"].fillna("").astype("string")

    df["missing_coordinate_flag"] = latitude.isna() | longitude.isna()
    df["zero_coordinate_flag"] = (latitude == 0) | (longitude == 0)
    df["longitude_positive_flag"] = longitude > 0
    df["out_of_state_flag"] = (state != "") & (state != "CA")
    df["implausible_coordinate_flag"] = (
        latitude.notna()
        & longitude.notna()
        & ((latitude < 32) | (latitude > 43) | (longitude > -114) | (longitude < -125))
    )
    return df


def make_invalid_numeric_flags(df: pd.DataFrame) -> pd.DataFrame:
    df["invalid_closeprice_flag"] = df["ClosePrice"].notna() & (df["ClosePrice"] <= 0)
    df["invalid_livingarea_flag"] = df["LivingArea"].notna() & (df["LivingArea"] <= 0)
    df["invalid_daysonmarket_flag"] = df["DaysOnMarket"].notna() & (df["DaysOnMarket"] < 0)
    df["invalid_bedrooms_flag"] = df["BedroomsTotal"].notna() & (df["BedroomsTotal"] < 0)
    df["invalid_bathrooms_flag"] = (
        df["BathroomsTotalInteger"].notna() & (df["BathroomsTotalInteger"] < 0)
    )
    return df


def build_missing_summary(df: pd.DataFrame) -> pd.DataFrame:
    missing_summary = pd.DataFrame(
        {
            "column": df.columns,
            "missing_count": df.isna().sum().values,
            "missing_percent": (df.isna().mean() * 100).round(2).values,
        }
    )
    return missing_summary.sort_values(
        ["missing_percent", "missing_count", "column"], ascending=[False, False, True]
    )


def build_date_flag_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for column in [
        "listing_after_close_flag",
        "purchase_after_close_flag",
        "negative_timeline_flag",
    ]:
        rows.append({"flag_name": column, "flag_count": int(df[column].sum())})
    return pd.DataFrame(rows)


def build_geo_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for column in [
        "missing_coordinate_flag",
        "zero_coordinate_flag",
        "longitude_positive_flag",
        "out_of_state_flag",
        "implausible_coordinate_flag",
    ]:
        rows.append({"flag_name": column, "flag_count": int(df[column].sum())})
    return pd.DataFrame(rows)


def build_invalid_numeric_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for column in [
        "invalid_closeprice_flag",
        "invalid_livingarea_flag",
        "invalid_daysonmarket_flag",
        "invalid_bedrooms_flag",
        "invalid_bathrooms_flag",
    ]:
        rows.append({"flag_name": column, "flag_count": int(df[column].sum())})
    return pd.DataFrame(rows)


def build_dtype_confirmation(df: pd.DataFrame) -> pd.DataFrame:
    dtypes_df = df.dtypes.astype(str).reset_index()
    dtypes_df.columns = ["column", "dtype"]
    return dtypes_df


def transformation_rows(dataset_name: str, drop_columns: Iterable[str]) -> List[dict]:
    rows = [
        {
            "step": 1,
            "transformation": "load_week23_output",
            "reason": "Continue Week 4-5 work on top of last week's residential and mortgage-enriched dataset without modifying Week 2-3 code.",
            "dataset": dataset_name,
        },
        {
            "step": 2,
            "transformation": "convert_dates_to_datetime",
            "reason": "Required for date consistency checks and timeline validation.",
            "dataset": dataset_name,
        },
        {
            "step": 3,
            "transformation": "convert_numeric_fields",
            "reason": "Ensure numeric columns are analysis-ready and can be validated.",
            "dataset": dataset_name,
        },
        {
            "step": 4,
            "transformation": "strip_blank_strings",
            "reason": "Normalize empty strings into missing values before cleaning.",
            "dataset": dataset_name,
        },
        {
            "step": 5,
            "transformation": "drop_unnecessary_columns",
            "reason": "Remove metadata and non-essential descriptive fields from the analysis-ready dataset.",
            "dataset": dataset_name,
            "details": ", ".join(drop_columns),
        },
        {
            "step": 6,
            "transformation": "add_date_consistency_flags",
            "reason": "Flag records where the timeline order of listing, purchase, and close dates is inconsistent.",
            "dataset": dataset_name,
        },
        {
            "step": 7,
            "transformation": "add_geographic_quality_flags",
            "reason": "Flag missing, zero, out-of-state, and implausible coordinates for California records.",
            "dataset": dataset_name,
        },
        {
            "step": 8,
            "transformation": "add_invalid_numeric_flags",
            "reason": "Flag invalid numeric values such as non-positive prices, non-positive living area, and negative DaysOnMarket.",
            "dataset": dataset_name,
        },
        {
            "step": 9,
            "transformation": "remove_critical_invalid_rows",
            "reason": "Drop rows with invalid numeric values or missing critical fields so the final dataset is analysis-ready.",
            "dataset": dataset_name,
        },
    ]
    return rows


def required_columns(dataset_name: str) -> List[str]:
    common = ["ListingId", "ListingContractDate", "ListPrice"]
    if dataset_name == "sold":
        return common + ["CloseDate", "ClosePrice"]
    return common


def drop_columns_for(dataset_name: str, columns: Iterable[str]) -> List[str]:
    drops = list(COMMON_DROP_COLUMNS)
    if dataset_name == "sold":
        drops.extend(SOLD_ONLY_DROP_COLUMNS)
    else:
        drops.extend(LISTED_ONLY_DROP_COLUMNS)
    return [column for column in drops if column in set(columns)]


def remove_critical_invalid_rows(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    required_missing_mask = pd.Series(False, index=df.index)
    for column in required_columns(dataset_name):
        required_missing_mask = required_missing_mask | df[column].isna()

    invalid_mask = (
        df["invalid_closeprice_flag"]
        | df["invalid_livingarea_flag"]
        | df["invalid_daysonmarket_flag"]
        | df["invalid_bedrooms_flag"]
        | df["invalid_bathrooms_flag"]
        | required_missing_mask
    )
    return df.loc[~invalid_mask].copy()


def build_row_count_summary(
    rows_before: int,
    rows_after: int,
    date_summary: pd.DataFrame,
    geo_summary: pd.DataFrame,
    invalid_numeric_summary: pd.DataFrame,
) -> pd.DataFrame:
    rows = [
        {"metric": "rows_before_cleaning", "value": rows_before},
        {"metric": "rows_after_cleaning", "value": rows_after},
        {"metric": "rows_removed", "value": rows_before - rows_after},
    ]
    for _, row in invalid_numeric_summary.iterrows():
        rows.append({"metric": row["flag_name"], "value": int(row["flag_count"])})
    for _, row in date_summary.iterrows():
        rows.append({"metric": row["flag_name"], "value": int(row["flag_count"])})
    for _, row in geo_summary.iterrows():
        rows.append({"metric": row["flag_name"], "value": int(row["flag_count"])})
    return pd.DataFrame(rows)


def run_week45_cleaning(dataset_name: str) -> Dict[str, Path]:
    paths = dataset_paths(dataset_name)

    original_df = load_dataset(dataset_name)
    working_df = original_df.copy()
    working_df = clean_strings(working_df)
    working_df = convert_types(working_df)

    columns_to_drop = drop_columns_for(dataset_name, working_df.columns)
    working_df = working_df.drop(columns=columns_to_drop)

    working_df = make_date_flags(working_df)
    working_df = make_geographic_flags(working_df)
    working_df = make_invalid_numeric_flags(working_df)

    date_summary = build_date_flag_summary(working_df)
    geo_summary = build_geo_summary(working_df)
    invalid_numeric_summary = build_invalid_numeric_summary(working_df)

    cleaned_df = remove_critical_invalid_rows(working_df, dataset_name)
    missing_summary = build_missing_summary(cleaned_df)
    dtype_confirmation = build_dtype_confirmation(cleaned_df)
    row_count_summary = build_row_count_summary(
        len(original_df), len(cleaned_df), date_summary, geo_summary, invalid_numeric_summary
    )
    transformation_log = pd.DataFrame(transformation_rows(dataset_name, columns_to_drop))

    cleaned_df.to_csv(paths["cleaned"], index=False)
    row_count_summary.to_csv(paths["row_counts"], index=False)
    transformation_log.to_csv(paths["transformation_log"], index=False)
    dtype_confirmation.to_csv(paths["dtypes"], index=False)
    date_summary.to_csv(paths["date_flags"], index=False)
    geo_summary.to_csv(paths["geo_summary"], index=False)
    missing_summary.to_csv(paths["missing_summary"], index=False)
    invalid_numeric_summary.to_csv(paths["invalid_numeric_summary"], index=False)

    print(f"\n===== WEEK 4-5 {dataset_name.upper()} CLEANING =====")
    print(f"Rows before cleaning: {len(original_df):,}")
    print(f"Rows after cleaning: {len(cleaned_df):,}")
    print("\nDate flag counts:")
    print(date_summary.to_string(index=False))
    print("\nGeographic quality summary:")
    print(geo_summary.to_string(index=False))
    print("\nInvalid numeric summary:")
    print(invalid_numeric_summary.to_string(index=False))
    print(f"\nSaved outputs to: {paths['output_root']}")

    return paths
