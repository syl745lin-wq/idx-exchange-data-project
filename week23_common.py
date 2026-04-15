from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib-cache")
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "output"
WEEK23_DIR = OUTPUT_DIR / "week2_3"

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

CORE_FIELDS = set(MARKET_ANALYSIS_FIELDS + [
    "MlsStatus",
    "Latitude",
    "Longitude",
    "LotSizeSquareFeet",
    "UnparsedAddress",
])


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


def load_and_concat(pattern: str, dataset_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
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

    print(f"\n===== {dataset_name.upper()} APPEND CHECK =====")
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

    missing_report = pd.DataFrame({
        "column": df.columns,
        "missing_count": df.isna().sum().values,
        "missing_percent": (df.isna().mean() * 100).round(2).values,
    }).sort_values(["missing_percent", "missing_count"], ascending=[False, False])

    high_missing = missing_report[missing_report["missing_percent"] > 90].copy()

    market_fields_found = pd.DataFrame({
        "market_analysis_fields_found": [col for col in MARKET_ANALYSIS_FIELDS if col in df.columns]
    })
    metadata_fields_found = pd.DataFrame({
        "metadata_fields_found": [col for col in METADATA_FIELDS if col in df.columns]
    })

    return {
        "dtypes": dtypes_df,
        "missing": missing_report,
        "high_missing": high_missing,
        "market_fields": market_fields_found,
        "metadata_fields": metadata_fields_found,
    }


def build_retention_report(df: pd.DataFrame, missing_report: pd.DataFrame) -> pd.DataFrame:
    missing_lookup = missing_report.set_index("column")
    rows = []
    for column in df.columns:
        missing_percent = float(missing_lookup.loc[column, "missing_percent"])
        if column in CORE_FIELDS:
            action = "retain_core"
            reason = "Core market-analysis field retained for Week 2-3 deliverables."
        elif missing_percent > 90:
            action = "drop_candidate"
            reason = ">90% missing and not needed for core analysis."
        else:
            action = "retain"
            reason = "Useful field or acceptable completeness for exploration."
        rows.append({
            "column": column,
            "missing_percent": missing_percent,
            "action": action,
            "reason": reason,
        })
    return pd.DataFrame(rows).sort_values(["action", "missing_percent", "column"], ascending=[True, False, True])


def filter_residential(df: pd.DataFrame, dataset_name: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    property_counts_before = df["PropertyType"].fillna("<MISSING>").value_counts(dropna=False).rename_axis("PropertyType").reset_index(name="row_count")

    print(f"\n===== {dataset_name.upper()} RESIDENTIAL FILTER CHECK =====")
    print(f"Row count before Residential filter: {len(df):,}")
    print("PropertyType frequency before filtering:")
    print(property_counts_before.to_string(index=False))

    residential_df = df[df["PropertyType"] == "Residential"].copy()
    property_counts_after = residential_df["PropertyType"].fillna("<MISSING>").value_counts(dropna=False).rename_axis("PropertyType").reset_index(name="row_count")

    print(f"\nRow count after Residential filter: {len(residential_df):,}")
    print("PropertyType frequency after filtering:")
    print(property_counts_after.to_string(index=False))

    filter_validation = pd.DataFrame([
        {"metric": "rows_before_filter", "value": len(df)},
        {"metric": "rows_after_filter", "value": len(residential_df)},
        {"metric": "rows_removed", "value": len(df) - len(residential_df)},
        {"metric": "residential_share_before_filter_pct", "value": round((len(residential_df) / len(df)) * 100, 2)},
    ])
    return residential_df, property_counts_before, property_counts_after, filter_validation


def select_retained_columns(df: pd.DataFrame, retention_report: pd.DataFrame) -> pd.DataFrame:
    retained = retention_report[retention_report["action"] != "drop_candidate"]["column"].tolist()
    return df[retained].copy()


def numeric_distribution_summary(df: pd.DataFrame) -> pd.DataFrame:
    available_fields = [field for field in NUMERIC_FIELDS if field in df.columns]
    if not available_fields:
        return pd.DataFrame()

    summary = df[available_fields].describe(percentiles=[0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99]).T
    summary.insert(0, "field", summary.index)
    summary = summary.reset_index(drop=True)
    return summary


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
        rows.append({
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
        })
    return pd.DataFrame(rows)


def save_numeric_plots(df: pd.DataFrame, dataset_slug: str, plot_dir: Path) -> None:
    ensure_dir(plot_dir)
    for field in [f for f in NUMERIC_FIELDS if f in df.columns]:
        series = df[field].dropna()
        if series.empty:
            continue

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(series, bins=40, color="#2F6B9A", edgecolor="white")
        ax.set_title(f"{dataset_slug} histogram: {field}")
        ax.set_xlabel(field)
        ax.set_ylabel("count")
        fig.tight_layout()
        fig.savefig(plot_dir / f"{dataset_slug}_{field}_hist.png", dpi=150)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(8, 2.8))
        ax.boxplot(series, vert=False)
        ax.set_title(f"{dataset_slug} boxplot: {field}")
        ax.set_xlabel(field)
        fig.tight_layout()
        fig.savefig(plot_dir / f"{dataset_slug}_{field}_box.png", dpi=150)
        plt.close(fig)


def build_question_answers(df_before_filter: pd.DataFrame, residential_df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    rows = []

    residential_count = int((df_before_filter["PropertyType"] == "Residential").sum())
    total_count = len(df_before_filter)
    rows.append({
        "question": "What is the Residential vs. other property type share?",
        "answer": f"Residential: {residential_count:,} of {total_count:,} rows ({(residential_count / total_count) * 100:.2f}%). Other property types: {total_count - residential_count:,} ({((total_count - residential_count) / total_count) * 100:.2f}%).",
    })

    if "ClosePrice" in residential_df.columns:
        close_price = residential_df["ClosePrice"].dropna()
        if not close_price.empty:
            rows.append({
                "question": "What are the median and average close prices?",
                "answer": f"Median ClosePrice: {close_price.median():,.2f}; Average ClosePrice: {close_price.mean():,.2f}.",
            })

    if "DaysOnMarket" in residential_df.columns:
        dom = residential_df["DaysOnMarket"].dropna()
        if not dom.empty:
            rows.append({
                "question": "What does the Days on Market distribution look like?",
                "answer": f"Median DOM: {dom.median():.2f}; Mean DOM: {dom.mean():.2f}; 95th percentile DOM: {dom.quantile(0.95):.2f}; Max DOM: {dom.max():.2f}.",
            })

    if {"ClosePrice", "ListPrice"}.issubset(residential_df.columns):
        price_compare = residential_df[["ClosePrice", "ListPrice"]].dropna()
        if not price_compare.empty:
            above = int((price_compare["ClosePrice"] > price_compare["ListPrice"]).sum())
            below = int((price_compare["ClosePrice"] < price_compare["ListPrice"]).sum())
            equal = int((price_compare["ClosePrice"] == price_compare["ListPrice"]).sum())
            total = len(price_compare)
            rows.append({
                "question": "What percentage of homes sold above vs. below list price?",
                "answer": f"Above list: {above:,} ({(above / total) * 100:.2f}%); Below list: {below:,} ({(below / total) * 100:.2f}%); Equal to list: {equal:,} ({(equal / total) * 100:.2f}%).",
            })

    if {"CloseDate", "ListingContractDate"}.issubset(residential_df.columns):
        valid_dates = residential_df[["CloseDate", "ListingContractDate"]].dropna()
        if not valid_dates.empty:
            inconsistent = int((valid_dates["CloseDate"] < valid_dates["ListingContractDate"]).sum())
            rows.append({
                "question": "Are there any apparent date consistency issues (e.g., close date before listing date)?",
                "answer": f"Rows with CloseDate earlier than ListingContractDate: {inconsistent:,}.",
            })

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
                rows.append({
                    "question": "Which counties have the highest median prices?",
                    "answer": county_answer,
                })

    return pd.DataFrame(rows)


def run_week23_analysis(dataset_name: str, raw_pattern: str, combined_filename: str, filtered_filename: str) -> Dict[str, Path]:
    dataset_slug = dataset_name.lower()
    dataset_dir = WEEK23_DIR / dataset_slug
    plot_dir = dataset_dir / "plots"
    ensure_dir(dataset_dir)
    ensure_dir(plot_dir)

    combined_df, row_count_df = load_and_concat(raw_pattern, dataset_name)
    combined_df = convert_types(combined_df)

    reports = summarize_dataset(combined_df)
    retention_report = build_retention_report(combined_df, reports["missing"])

    residential_df, property_before, property_after, filter_validation = filter_residential(combined_df, dataset_name)
    filtered_output_df = select_retained_columns(residential_df, retention_report)

    numeric_summary = numeric_distribution_summary(filtered_output_df)
    outliers = outlier_summary(filtered_output_df)
    question_answers = build_question_answers(combined_df, filtered_output_df, dataset_name)

    combined_path = dataset_dir / combined_filename
    filtered_path = dataset_dir / filtered_filename
    dtypes_path = dataset_dir / f"{dataset_slug}_dtypes.csv"
    missing_path = dataset_dir / f"{dataset_slug}_missing_value_report.csv"
    high_missing_path = dataset_dir / f"{dataset_slug}_high_missing_gt90.csv"
    retention_path = dataset_dir / f"{dataset_slug}_column_retention_report.csv"
    numeric_path = dataset_dir / f"{dataset_slug}_numeric_distribution_summary.csv"
    outlier_path = dataset_dir / f"{dataset_slug}_outlier_summary.csv"
    property_before_path = dataset_dir / f"{dataset_slug}_property_type_before_filter.csv"
    property_after_path = dataset_dir / f"{dataset_slug}_property_type_after_filter.csv"
    filter_validation_path = dataset_dir / f"{dataset_slug}_filter_validation.csv"
    questions_path = dataset_dir / f"{dataset_slug}_intern_questions.csv"
    row_counts_path = dataset_dir / f"{dataset_slug}_row_counts.csv"

    combined_df.to_csv(combined_path, index=False)
    filtered_output_df.to_csv(filtered_path, index=False)
    reports["dtypes"].to_csv(dtypes_path, index=False)
    reports["missing"].to_csv(missing_path, index=False)
    reports["high_missing"].to_csv(high_missing_path, index=False)
    retention_report.to_csv(retention_path, index=False)
    numeric_summary.to_csv(numeric_path, index=False)
    outliers.to_csv(outlier_path, index=False)
    property_before.to_csv(property_before_path, index=False)
    property_after.to_csv(property_after_path, index=False)
    filter_validation.to_csv(filter_validation_path, index=False)
    question_answers.to_csv(questions_path, index=False)
    row_count_df.to_csv(row_counts_path, index=False)

    save_numeric_plots(filtered_output_df, dataset_slug, plot_dir)

    print(f"\n===== {dataset_name.upper()} DATASET UNDERSTANDING =====")
    print(f"Shape: {combined_df.shape}")
    print("Columns available:")
    print(list(combined_df.columns))
    print("\nHigh-missing columns (>90% null):")
    print(reports["high_missing"].to_string(index=False))
    print("\nMarket analysis fields found:")
    print(reports["market_fields"].to_string(index=False))
    print("\nMetadata fields found:")
    print(reports["metadata_fields"].to_string(index=False))
    print("\nNumeric distribution summary:")
    print(numeric_summary.to_string(index=False))
    print("\nSuggested intern questions summary:")
    print(question_answers.to_string(index=False))

    return {
        "combined_path": combined_path,
        "filtered_path": filtered_path,
        "dataset_dir": dataset_dir,
    }
