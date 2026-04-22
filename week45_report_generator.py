from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from week23_common import BASE_DIR, ensure_dir

WEEK45_DIR = BASE_DIR / "output" / "week4_5"
REPORT_DIR = BASE_DIR / "report_assets" / "week45"
REPORT_MD = BASE_DIR / "week45_summary_report.md"


def read_summary(dataset_name: str, file_name: str) -> pd.DataFrame:
    return pd.read_csv(WEEK45_DIR / dataset_name / file_name)


def metric(df: pd.DataFrame, name: str) -> int:
    return int(df.loc[df["metric"] == name, "value"].iloc[0])


def generate_cleanup_chart() -> Path:
    sold_rows = read_summary("sold", "sold_week45_row_counts.csv")
    listed_rows = read_summary("listed", "listed_week45_row_counts.csv")

    before = [
        metric(sold_rows, "rows_before_cleaning"),
        metric(listed_rows, "rows_before_cleaning"),
    ]
    after = [
        metric(sold_rows, "rows_after_cleaning"),
        metric(listed_rows, "rows_after_cleaning"),
    ]
    removed = [before[i] - after[i] for i in range(2)]

    fig, ax = plt.subplots(figsize=(8, 4.8))
    labels = ["Sold", "Listed"]
    ax.bar(labels, after, color="#2F6B9A", label="Retained After Cleaning")
    ax.bar(labels, removed, bottom=after, color="#D66B4E", label="Removed")
    ax.set_title("Week 4-5 Cleaning: Rows Before vs After")
    ax.set_ylabel("Row Count")
    ax.legend()
    for i, (a, r) in enumerate(zip(after, removed)):
        ax.text(i, a / 2, f"{a:,}", ha="center", va="center", color="white", fontweight="bold")
        ax.text(i, a + max(r / 2, 1), f"{r:,}", ha="center", va="center", color="black")
    fig.tight_layout()
    output_path = REPORT_DIR / "week45_row_cleanup.png"
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return output_path


def generate_geo_chart() -> Path:
    sold_geo = read_summary("sold", "sold_geographic_quality_summary.csv")
    listed_geo = read_summary("listed", "listed_geographic_quality_summary.csv")

    flags = sold_geo["flag_name"].tolist()
    sold_values = sold_geo["flag_count"].tolist()
    listed_values = listed_geo["flag_count"].tolist()

    fig, ax = plt.subplots(figsize=(10, 5.2))
    positions = range(len(flags))
    width = 0.36

    ax.bar([p - width / 2 for p in positions], sold_values, width=width, label="Sold", color="#2F6B9A")
    ax.bar([p + width / 2 for p in positions], listed_values, width=width, label="Listed", color="#D6A94E")
    ax.set_title("Week 4-5 Geographic Data Quality Flags")
    ax.set_ylabel("Flag Count")
    ax.set_xticks(list(positions))
    ax.set_xticklabels(
        [
            "Missing\ncoord",
            "Zero\ncoord",
            "Positive\nlon",
            "Out-of-\nstate",
            "Implausible\ncoord",
        ]
    )
    ax.legend()
    fig.tight_layout()
    output_path = REPORT_DIR / "week45_geo_flags.png"
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return output_path


def write_markdown_report() -> None:
    sold_rows = read_summary("sold", "sold_week45_row_counts.csv")
    listed_rows = read_summary("listed", "listed_week45_row_counts.csv")
    sold_dates = read_summary("sold", "sold_date_flag_summary.csv")
    listed_dates = read_summary("listed", "listed_date_flag_summary.csv")
    sold_geo = read_summary("sold", "sold_geographic_quality_summary.csv")
    listed_geo = read_summary("listed", "listed_geographic_quality_summary.csv")

    text = f"""# Week 4-5 Report

## Scope
Week 4-5 focused on data cleaning and preparation using the Week 2-3 residential, mortgage-enriched datasets as inputs. No Week 2-3 Python files were changed. Instead, new Week 4-5 scripts were added to create analysis-ready sold and listed datasets.

## Cleaning Actions
- Converted `CloseDate`, `PurchaseContractDate`, `ListingContractDate`, and `ContractStatusChangeDate` to datetime.
- Confirmed numeric typing for price, area, bedroom, bathroom, DaysOnMarket, coordinates, and mortgage-rate fields.
- Removed unnecessary metadata and descriptive columns from the final analysis-ready outputs.
- Kept optional missing values as null, but removed rows with missing critical fields or invalid numeric values.
- Added date consistency flags:
  - `listing_after_close_flag`
  - `purchase_after_close_flag`
  - `negative_timeline_flag`
- Added geographic quality flags:
  - `missing_coordinate_flag`
  - `zero_coordinate_flag`
  - `longitude_positive_flag`
  - `out_of_state_flag`
  - `implausible_coordinate_flag`

## Before and After Row Counts
- Sold: {metric(sold_rows, "rows_before_cleaning"):,} before cleaning and {metric(sold_rows, "rows_after_cleaning"):,} after cleaning.
- Listed: {metric(listed_rows, "rows_before_cleaning"):,} before cleaning and {metric(listed_rows, "rows_after_cleaning"):,} after cleaning.

## Date Consistency Results
### Sold
{sold_dates.to_markdown(index=False)}

### Listed
{listed_dates.to_markdown(index=False)}

## Geographic Quality Results
### Sold
{sold_geo.to_markdown(index=False)}

### Listed
{listed_geo.to_markdown(index=False)}

## Deliverables
- `output/week4_5/sold/sold_analysis_ready_week45.csv`
- `output/week4_5/listed/listed_analysis_ready_week45.csv`
- Row count summaries, dtype confirmations, missing summaries, date flag summaries, geographic quality summaries, and transformation logs for both datasets.
"""
    REPORT_MD.write_text(text, encoding="utf-8")


def main() -> None:
    ensure_dir(REPORT_DIR)
    generate_cleanup_chart()
    generate_geo_chart()
    write_markdown_report()
    print(f"Saved Week 4-5 report assets to: {REPORT_DIR}")
    print(f"Saved Week 4-5 markdown report to: {REPORT_MD}")


if __name__ == "__main__":
    main()
