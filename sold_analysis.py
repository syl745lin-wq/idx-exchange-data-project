from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-cache")
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "output"
WEEK23_DIR = OUTPUT_DIR / "week2_3" / "sold"
MORTGAGE_DIR = OUTPUT_DIR / "week2_3" / "mortgage_enrichment"
WEEK45_DIR = OUTPUT_DIR / "week4_5" / "sold"
FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=MORTGAGE30US"

NUMERIC_FIELDS = [
    "ClosePrice",
    "ListPrice",
    "OriginalListPrice",
    "LivingArea",
    "LotSizeAcres",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "DaysOnMarket",
    "YearBuilt",
]
DATE_FIELDS = [
    "CloseDate",
    "ListingContractDate",
    "PurchaseContractDate",
    "ContractStatusChangeDate",
]
MARKET_ANALYSIS_FIELDS = [
    "ListingId",
    "PropertyType",
    "PropertySubType",
    "City",
    "CountyOrParish",
    "PostalCode",
    "StateOrProvince",
    "CloseDate",
    "ListingContractDate",
    "ClosePrice",
    "ListPrice",
    "OriginalListPrice",
    "LivingArea",
    "LotSizeAcres",
    "BedroomsTotal",
    "BathroomsTotalInteger",
    "DaysOnMarket",
    "YearBuilt",
]
METADATA_FIELDS = [
    "ListingKey",
    "ListingKeyNumeric",
    "ListAgentEmail",
    "ListAgentFirstName",
    "ListAgentLastName",
    "ListAgentFullName",
    "BuyerAgentFirstName",
    "BuyerAgentLastName",
    "BuyerOfficeName",
    "ListOfficeName",
    "CoListOfficeName",
    "OriginatingSystemName",
    "OriginatingSystemSubName",
]
CORE_FIELDS = set(
    MARKET_ANALYSIS_FIELDS
    + ["MlsStatus", "Latitude", "Longitude", "LotSizeSquareFeet", "UnparsedAddress"]
)

WEEK45_DATE_COLUMNS = [
    "CloseDate",
    "PurchaseContractDate",
    "ListingContractDate",
    "ContractStatusChangeDate",
]
WEEK45_NUMERIC_COLUMNS = [
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
WEEK45_DROP_COLUMNS = [
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
    "OriginatingSystemName",
    "OriginatingSystemSubName",
    "BuyerAgentAOR",
    "ListAgentAOR",
    "Flooring",
    "ViewYN",
    "latfilled",
    "lonfilled",
]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def merge_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    duplicate_cols = [col for col in df.columns if col.endswith(".1")]
    for dup_col in duplicate_cols:
        base_col = dup_col[:-2]
        if base_col in df.columns:
            df[base_col] = df[base_col].where(df[base_col].notna(), df[dup_col])
            df = df.drop(columns=[dup_col])
        else:
            df = df.rename(columns={dup_col: base_col})
    return df


def load_and_concat(pattern: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    files = sorted(RAW_DIR.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found for pattern {pattern} in {RAW_DIR}")

    frames: List[pd.DataFrame] = []
    row_counts = []
    for file_path in files:
        frame = pd.read_csv(file_path, low_memory=False)
        frames.append(frame)
        row_counts.append({"file_name": file_path.name, "row_count_before_append": len(frame)})

    combined = pd.concat(frames, ignore_index=True)
    combined = merge_duplicate_columns(combined)

    row_count_df = pd.DataFrame(row_counts)
    row_count_df.loc[len(row_count_df)] = {
        "file_name": "COMBINED_AFTER_CONCAT",
        "row_count_before_append": len(combined),
    }

    print("\n===== SOLD APPEND CHECK =====")
    print("Row count for each individual file before append:")
    print(row_count_df[row_count_df["file_name"] != "COMBINED_AFTER_CONCAT"].to_string(index=False))
    print(f"\nCombined/appended dataset row count after concatenation: {len(combined):,}")
    return combined, row_count_df


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in NUMERIC_FIELDS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in DATE_FIELDS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def summarize_dataset(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    dtypes_df = df.dtypes.astype(str).reset_index()
    dtypes_df.columns = ["column", "data_type"]
    missing_report = pd.DataFrame(
        {
            "column": df.columns,
            "missing_count": df.isna().sum().values,
            "missing_percent": (df.isna().mean() * 100).round(2).values,
        }
    ).sort_values(["missing_percent", "missing_count"], ascending=[False, False])
    high_missing = missing_report[missing_report["missing_percent"] > 90].copy()
    market_fields_found = pd.DataFrame(
        {"market_analysis_fields_found": [col for col in MARKET_ANALYSIS_FIELDS if col in df.columns]}
    )
    metadata_fields_found = pd.DataFrame(
        {"metadata_fields_found": [col for col in METADATA_FIELDS if col in df.columns]}
    )
    return {
        "dtypes": dtypes_df,
        "missing": missing_report,
        "high_missing": high_missing,
        "market_fields": market_fields_found,
        "metadata_fields": metadata_fields_found,
    }


def build_retention_report(df: pd.DataFrame, missing_report: pd.DataFrame) -> pd.DataFrame:
    lookup = missing_report.set_index("column")
    rows = []
    for column in df.columns:
        missing_percent = float(lookup.loc[column, "missing_percent"])
        if column in CORE_FIELDS:
            action = "retain_core"
            reason = "Core market-analysis field retained for Week 2-3 deliverables."
        elif missing_percent > 90:
            action = "drop_candidate"
            reason = ">90% missing and not needed for core analysis."
        else:
            action = "retain"
            reason = "Useful field or acceptable completeness for exploration."
        rows.append(
            {
                "column": column,
                "missing_percent": missing_percent,
                "action": action,
                "reason": reason,
            }
        )
    return pd.DataFrame(rows).sort_values(["action", "missing_percent", "column"])


def filter_residential(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    property_counts_before = (
        df["PropertyType"].fillna("<MISSING>").value_counts(dropna=False).rename_axis("PropertyType").reset_index(name="row_count")
    )
    print("\n===== SOLD RESIDENTIAL FILTER CHECK =====")
    print(f"Row count before Residential filter: {len(df):,}")
    print("PropertyType frequency before filtering:")
    print(property_counts_before.to_string(index=False))

    residential_df = df[df["PropertyType"] == "Residential"].copy()
    property_counts_after = (
        residential_df["PropertyType"]
        .fillna("<MISSING>")
        .value_counts(dropna=False)
        .rename_axis("PropertyType")
        .reset_index(name="row_count")
    )
    print(f"\nRow count after Residential filter: {len(residential_df):,}")
    print("PropertyType frequency after filtering:")
    print(property_counts_after.to_string(index=False))

    validation = pd.DataFrame(
        [
            {"metric": "rows_before_filter", "value": len(df)},
            {"metric": "rows_after_filter", "value": len(residential_df)},
            {"metric": "rows_removed", "value": len(df) - len(residential_df)},
            {
                "metric": "residential_share_before_filter_pct",
                "value": round((len(residential_df) / len(df)) * 100, 2),
            },
        ]
    )
    return residential_df, property_counts_before, property_counts_after, validation


def select_retained_columns(df: pd.DataFrame, retention_report: pd.DataFrame) -> pd.DataFrame:
    retained = retention_report[retention_report["action"] != "drop_candidate"]["column"].tolist()
    return df[retained].copy()


def numeric_distribution_summary(df: pd.DataFrame) -> pd.DataFrame:
    available_fields = [field for field in NUMERIC_FIELDS if field in df.columns]
    if not available_fields:
        return pd.DataFrame()
    summary = df[available_fields].describe(percentiles=[0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99]).T
    summary.insert(0, "field", summary.index)
    return summary.reset_index(drop=True)


def outlier_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for field in [f for f in NUMERIC_FIELDS if f in df.columns]:
        series = df[field].dropna()
        if series.empty:
            continue
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        rows.append(
            {
                "field": field,
                "count_non_null": int(series.count()),
                "min": float(series.min()),
                "p01": float(series.quantile(0.01)),
                "median": float(series.median()),
                "p99": float(series.quantile(0.99)),
                "max": float(series.max()),
                "iqr_lower_bound": float(lower),
                "iqr_upper_bound": float(upper),
                "rows_below_lower_bound": int((series < lower).sum()),
                "rows_above_upper_bound": int((series > upper).sum()),
            }
        )
    return pd.DataFrame(rows)


def save_numeric_plots(df: pd.DataFrame, plot_dir: Path) -> None:
    ensure_dir(plot_dir)
    for field in [f for f in NUMERIC_FIELDS if f in df.columns]:
        series = df[field].dropna()
        if series.empty:
            continue
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(series, bins=40, color="#2F6B9A", edgecolor="white")
        ax.set_title(f"sold histogram: {field}")
        ax.set_xlabel(field)
        ax.set_ylabel("count")
        fig.tight_layout()
        fig.savefig(plot_dir / f"sold_{field}_hist.png", dpi=150)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(8, 2.8))
        ax.boxplot(series, vert=False)
        ax.set_title(f"sold boxplot: {field}")
        ax.set_xlabel(field)
        fig.tight_layout()
        fig.savefig(plot_dir / f"sold_{field}_box.png", dpi=150)
        plt.close(fig)


def build_question_answers(df_before_filter: pd.DataFrame, residential_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    residential_count = int((df_before_filter["PropertyType"] == "Residential").sum())
    total_count = len(df_before_filter)
    rows.append(
        {
            "question": "What is the Residential vs. other property type share?",
            "answer": f"Residential: {residential_count:,} of {total_count:,} rows ({(residential_count / total_count) * 100:.2f}%). Other property types: {total_count - residential_count:,} ({((total_count - residential_count) / total_count) * 100:.2f}%).",
        }
    )
    close_price = residential_df["ClosePrice"].dropna()
    if not close_price.empty:
        rows.append(
            {
                "question": "What are the median and average close prices?",
                "answer": f"Median ClosePrice: {close_price.median():,.2f}; Average ClosePrice: {close_price.mean():,.2f}.",
            }
        )
    dom = residential_df["DaysOnMarket"].dropna()
    if not dom.empty:
        rows.append(
            {
                "question": "What does the Days on Market distribution look like?",
                "answer": f"Median DOM: {dom.median():.2f}; Mean DOM: {dom.mean():.2f}; 95th percentile DOM: {dom.quantile(0.95):.2f}; Max DOM: {dom.max():.2f}.",
            }
        )
    if {"ClosePrice", "ListPrice"}.issubset(residential_df.columns):
        price_compare = residential_df[["ClosePrice", "ListPrice"]].dropna()
        if not price_compare.empty:
            above = int((price_compare["ClosePrice"] > price_compare["ListPrice"]).sum())
            below = int((price_compare["ClosePrice"] < price_compare["ListPrice"]).sum())
            equal = int((price_compare["ClosePrice"] == price_compare["ListPrice"]).sum())
            total = len(price_compare)
            rows.append(
                {
                    "question": "What percentage of homes sold above vs. below list price?",
                    "answer": f"Above list: {above:,} ({(above / total) * 100:.2f}%); Below list: {below:,} ({(below / total) * 100:.2f}%); Equal to list: {equal:,} ({(equal / total) * 100:.2f}%).",
                }
            )
    if {"CloseDate", "ListingContractDate"}.issubset(residential_df.columns):
        valid_dates = residential_df[["CloseDate", "ListingContractDate"]].dropna()
        if not valid_dates.empty:
            inconsistent = int((valid_dates["CloseDate"] < valid_dates["ListingContractDate"]).sum())
            rows.append(
                {
                    "question": "Are there any apparent date consistency issues (e.g., close date before listing date)?",
                    "answer": f"Rows with CloseDate earlier than ListingContractDate: {inconsistent:,}.",
                }
            )
    if {"CountyOrParish", "ClosePrice"}.issubset(residential_df.columns):
        county_df = residential_df[["CountyOrParish", "ClosePrice"]].dropna()
        if not county_df.empty:
            county_summary = county_df.groupby("CountyOrParish").agg(
                median_close_price=("ClosePrice", "median"),
                transaction_count=("ClosePrice", "size"),
            )
            county_summary = county_summary[county_summary["transaction_count"] >= 25].sort_values(
                "median_close_price", ascending=False
            ).head(5)
            if not county_summary.empty:
                county_answer = "; ".join(
                    f"{idx}: median {row['median_close_price']:,.2f} (n={int(row['transaction_count'])})"
                    for idx, row in county_summary.iterrows()
                )
                rows.append(
                    {
                        "question": "Which counties have the highest median prices?",
                        "answer": county_answer,
                    }
                )
    return pd.DataFrame(rows)


def run_week23_analysis() -> Dict[str, Path]:
    ensure_dir(WEEK23_DIR)
    plot_dir = WEEK23_DIR / "plots"
    ensure_dir(plot_dir)

    combined_df, row_count_df = load_and_concat("CRMLSSold*.csv")
    combined_df = convert_types(combined_df)
    reports = summarize_dataset(combined_df)
    retention_report = build_retention_report(combined_df, reports["missing"])
    residential_df, property_before, property_after, filter_validation = filter_residential(combined_df)
    filtered_output_df = select_retained_columns(residential_df, retention_report)
    numeric_summary = numeric_distribution_summary(filtered_output_df)
    outliers = outlier_summary(filtered_output_df)
    question_answers = build_question_answers(combined_df, filtered_output_df)

    paths = {
        "combined": WEEK23_DIR / "sold_combined_week23.csv",
        "filtered": WEEK23_DIR / "sold_residential_week23.csv",
        "dtypes": WEEK23_DIR / "sold_dtypes.csv",
        "missing": WEEK23_DIR / "sold_missing_value_report.csv",
        "high_missing": WEEK23_DIR / "sold_high_missing_gt90.csv",
        "retention": WEEK23_DIR / "sold_column_retention_report.csv",
        "numeric": WEEK23_DIR / "sold_numeric_distribution_summary.csv",
        "outlier": WEEK23_DIR / "sold_outlier_summary.csv",
        "property_before": WEEK23_DIR / "sold_property_type_before_filter.csv",
        "property_after": WEEK23_DIR / "sold_property_type_after_filter.csv",
        "filter_validation": WEEK23_DIR / "sold_filter_validation.csv",
        "questions": WEEK23_DIR / "sold_intern_questions.csv",
        "row_counts": WEEK23_DIR / "sold_row_counts.csv",
    }
    combined_df.to_csv(paths["combined"], index=False)
    filtered_output_df.to_csv(paths["filtered"], index=False)
    reports["dtypes"].to_csv(paths["dtypes"], index=False)
    reports["missing"].to_csv(paths["missing"], index=False)
    reports["high_missing"].to_csv(paths["high_missing"], index=False)
    retention_report.to_csv(paths["retention"], index=False)
    numeric_summary.to_csv(paths["numeric"], index=False)
    outliers.to_csv(paths["outlier"], index=False)
    property_before.to_csv(paths["property_before"], index=False)
    property_after.to_csv(paths["property_after"], index=False)
    filter_validation.to_csv(paths["filter_validation"], index=False)
    question_answers.to_csv(paths["questions"], index=False)
    row_count_df.to_csv(paths["row_counts"], index=False)
    save_numeric_plots(filtered_output_df, plot_dir)
    return paths


def fetch_mortgage_rates() -> Tuple[pd.DataFrame, pd.DataFrame]:
    mortgage = pd.read_csv(FRED_URL)
    date_col = mortgage.columns[0]
    rate_col = mortgage.columns[1]
    mortgage = mortgage.rename(columns={date_col: "date", rate_col: "rate_30yr_fixed"})
    mortgage["date"] = pd.to_datetime(mortgage["date"], errors="coerce")
    mortgage["rate_30yr_fixed"] = pd.to_numeric(mortgage["rate_30yr_fixed"], errors="coerce")
    mortgage = mortgage.dropna(subset=["date", "rate_30yr_fixed"]).copy()
    mortgage["year_month"] = mortgage["date"].dt.to_period("M").astype(str)
    monthly = (
        mortgage.groupby("year_month", as_index=False)["rate_30yr_fixed"]
        .mean()
        .rename(columns={"rate_30yr_fixed": "mortgage30us_monthly_avg"})
    )
    return mortgage, monthly


def run_mortgage_enrichment(filtered_input: Path) -> Path:
    ensure_dir(MORTGAGE_DIR)
    sold = pd.read_csv(filtered_input, low_memory=False)
    weekly_rates, monthly_rates = fetch_mortgage_rates()
    sold["CloseDate"] = pd.to_datetime(sold["CloseDate"], errors="coerce")
    sold["year_month"] = sold["CloseDate"].dt.to_period("M").astype(str)
    sold_with_rates = sold.merge(monthly_rates, on="year_month", how="left")
    validation = pd.DataFrame(
        [
            {"metric": "input_rows", "value": len(sold)},
            {"metric": "output_rows", "value": len(sold_with_rates)},
            {"metric": "null_rate_values", "value": int(sold_with_rates["mortgage30us_monthly_avg"].isna().sum())},
        ]
    )
    weekly_rates.to_csv(MORTGAGE_DIR / "fred_mortgage30us_weekly.csv", index=False)
    monthly_rates.to_csv(MORTGAGE_DIR / "fred_mortgage30us_monthly.csv", index=False)
    sold_with_rates.to_csv(MORTGAGE_DIR / "sold_residential_with_mortgage_rates.csv", index=False)
    validation.to_csv(MORTGAGE_DIR / "sold_mortgage_merge_validation.csv", index=False)
    return MORTGAGE_DIR / "sold_residential_with_mortgage_rates.csv"


def week45_clean_strings(df: pd.DataFrame) -> pd.DataFrame:
    object_columns = df.select_dtypes(include=["object"]).columns
    for column in object_columns:
        df[column] = df[column].astype("string").str.strip()
        df[column] = df[column].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
    return df


def week45_convert_types(df: pd.DataFrame) -> pd.DataFrame:
    for column in WEEK45_DATE_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    for column in WEEK45_NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def make_date_flags(df: pd.DataFrame) -> pd.DataFrame:
    df["listing_after_close_flag"] = (
        df["ListingContractDate"].notna() & df["CloseDate"].notna() & (df["ListingContractDate"] > df["CloseDate"])
    )
    df["purchase_after_close_flag"] = (
        df["PurchaseContractDate"].notna() & df["CloseDate"].notna() & (df["PurchaseContractDate"] > df["CloseDate"])
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
    return missing_summary.sort_values(["missing_percent", "missing_count", "column"], ascending=[False, False, True])


def build_flag_summary(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    return pd.DataFrame([{"flag_name": column, "flag_count": int(df[column].sum())} for column in columns])


def build_dtype_confirmation(df: pd.DataFrame) -> pd.DataFrame:
    dtypes_df = df.dtypes.astype(str).reset_index()
    dtypes_df.columns = ["column", "dtype"]
    return dtypes_df


def transformation_rows(drop_columns: Iterable[str]) -> List[dict]:
    return [
        {
            "step": 1,
            "transformation": "load_week23_output",
            "reason": "Continue Week 4-5 work on top of the Week 2-3 residential and mortgage-enriched dataset.",
            "dataset": "sold",
        },
        {
            "step": 2,
            "transformation": "convert_dates_to_datetime",
            "reason": "Required for date consistency checks and timeline validation.",
            "dataset": "sold",
        },
        {
            "step": 3,
            "transformation": "convert_numeric_fields",
            "reason": "Ensure numeric columns are analysis-ready and can be validated.",
            "dataset": "sold",
        },
        {
            "step": 4,
            "transformation": "strip_blank_strings",
            "reason": "Normalize empty strings into missing values before cleaning.",
            "dataset": "sold",
        },
        {
            "step": 5,
            "transformation": "drop_unnecessary_columns",
            "reason": "Remove metadata and non-essential descriptive fields from the analysis-ready dataset.",
            "dataset": "sold",
            "details": ", ".join(drop_columns),
        },
        {
            "step": 6,
            "transformation": "add_date_consistency_flags",
            "reason": "Flag records where the timeline order of listing, purchase, and close dates is inconsistent.",
            "dataset": "sold",
        },
        {
            "step": 7,
            "transformation": "add_geographic_quality_flags",
            "reason": "Flag missing, zero, out-of-state, and implausible coordinates for California records.",
            "dataset": "sold",
        },
        {
            "step": 8,
            "transformation": "add_invalid_numeric_flags",
            "reason": "Flag invalid numeric values such as non-positive prices, non-positive living area, and negative DaysOnMarket.",
            "dataset": "sold",
        },
        {
            "step": 9,
            "transformation": "remove_critical_invalid_rows",
            "reason": "Drop rows with invalid numeric values or missing critical fields so the final dataset is analysis-ready.",
            "dataset": "sold",
        },
    ]


def remove_critical_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = ["ListingId", "ListingContractDate", "ListPrice", "CloseDate", "ClosePrice"]
    required_missing_mask = pd.Series(False, index=df.index)
    for column in required_columns:
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
    for summary_df in [invalid_numeric_summary, date_summary, geo_summary]:
        for _, row in summary_df.iterrows():
            rows.append({"metric": row["flag_name"], "value": int(row["flag_count"])})
    return pd.DataFrame(rows)


def run_week45_cleaning(input_path: Path) -> Dict[str, Path]:
    ensure_dir(WEEK45_DIR)
    original_df = pd.read_csv(input_path, low_memory=False)
    working_df = week45_clean_strings(original_df.copy())
    working_df = week45_convert_types(working_df)
    columns_to_drop = [column for column in WEEK45_DROP_COLUMNS if column in working_df.columns]
    working_df = working_df.drop(columns=columns_to_drop)
    working_df = make_date_flags(working_df)
    working_df = make_geographic_flags(working_df)
    working_df = make_invalid_numeric_flags(working_df)

    date_summary = build_flag_summary(
        working_df,
        ["listing_after_close_flag", "purchase_after_close_flag", "negative_timeline_flag"],
    )
    geo_summary = build_flag_summary(
        working_df,
        [
            "missing_coordinate_flag",
            "zero_coordinate_flag",
            "longitude_positive_flag",
            "out_of_state_flag",
            "implausible_coordinate_flag",
        ],
    )
    invalid_numeric_summary = build_flag_summary(
        working_df,
        [
            "invalid_closeprice_flag",
            "invalid_livingarea_flag",
            "invalid_daysonmarket_flag",
            "invalid_bedrooms_flag",
            "invalid_bathrooms_flag",
        ],
    )

    cleaned_df = remove_critical_invalid_rows(working_df)
    missing_summary = build_missing_summary(cleaned_df)
    dtype_confirmation = build_dtype_confirmation(cleaned_df)
    row_count_summary = build_row_count_summary(
        len(original_df), len(cleaned_df), date_summary, geo_summary, invalid_numeric_summary
    )
    transformation_log = pd.DataFrame(transformation_rows(columns_to_drop))

    paths = {
        "cleaned": WEEK45_DIR / "sold_analysis_ready_week45.csv",
        "row_counts": WEEK45_DIR / "sold_week45_row_counts.csv",
        "transformation_log": WEEK45_DIR / "sold_transformation_log.csv",
        "dtypes": WEEK45_DIR / "sold_week45_dtypes.csv",
        "date_flags": WEEK45_DIR / "sold_date_flag_summary.csv",
        "geo_summary": WEEK45_DIR / "sold_geographic_quality_summary.csv",
        "missing_summary": WEEK45_DIR / "sold_missing_summary.csv",
        "invalid_numeric_summary": WEEK45_DIR / "sold_invalid_numeric_summary.csv",
    }
    cleaned_df.to_csv(paths["cleaned"], index=False)
    row_count_summary.to_csv(paths["row_counts"], index=False)
    transformation_log.to_csv(paths["transformation_log"], index=False)
    dtype_confirmation.to_csv(paths["dtypes"], index=False)
    date_summary.to_csv(paths["date_flags"], index=False)
    geo_summary.to_csv(paths["geo_summary"], index=False)
    missing_summary.to_csv(paths["missing_summary"], index=False)
    invalid_numeric_summary.to_csv(paths["invalid_numeric_summary"], index=False)
    return paths


def main() -> None:
    week23_paths = run_week23_analysis()
    enriched_path = run_mortgage_enrichment(week23_paths["filtered"])
    run_week45_cleaning(enriched_path)
    print(f"\nSaved sold Week 2-3 outputs to: {WEEK23_DIR}")
    print(f"Saved sold Week 4-5 outputs to: {WEEK45_DIR}")


if __name__ == "__main__":
    main()
