from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
WEEK23_DIR = BASE_DIR / "output" / "week2_3"
OUTPUT_DIR = WEEK23_DIR / "mortgage_enrichment"
FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=MORTGAGE30US"
SOLD_INPUT = WEEK23_DIR / "sold" / "sold_residential_week23.csv"
LISTED_INPUT = WEEK23_DIR / "listed" / "listed_residential_week23.csv"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def fetch_mortgage_rates() -> tuple[pd.DataFrame, pd.DataFrame]:
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


def validate_inputs() -> None:
    missing = [str(path) for path in [SOLD_INPUT, LISTED_INPUT] if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Run sold_analysis.py and listed_analysis.py first. Missing required files: " + ", ".join(missing)
        )


def enrich_dataset(df: pd.DataFrame, date_column: str, monthly_rates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    df["year_month"] = df[date_column].dt.to_period("M").astype(str)

    enriched = df.merge(monthly_rates, on="year_month", how="left")
    validation = pd.DataFrame([
        {"metric": "input_rows", "value": len(df)},
        {"metric": "output_rows", "value": len(enriched)},
        {"metric": "null_rate_values", "value": int(enriched["mortgage30us_monthly_avg"].isna().sum())},
    ])
    return enriched, validation


def main() -> None:
    ensure_dir(OUTPUT_DIR)
    validate_inputs()

    sold = pd.read_csv(SOLD_INPUT, low_memory=False)
    listed = pd.read_csv(LISTED_INPUT, low_memory=False)

    weekly_rates, monthly_rates = fetch_mortgage_rates()
    sold_with_rates, sold_validation = enrich_dataset(sold, "CloseDate", monthly_rates)
    listed_with_rates, listed_validation = enrich_dataset(listed, "ListingContractDate", monthly_rates)

    weekly_rates.to_csv(OUTPUT_DIR / "fred_mortgage30us_weekly.csv", index=False)
    monthly_rates.to_csv(OUTPUT_DIR / "fred_mortgage30us_monthly.csv", index=False)
    sold_with_rates.to_csv(OUTPUT_DIR / "sold_residential_with_mortgage_rates.csv", index=False)
    listed_with_rates.to_csv(OUTPUT_DIR / "listed_residential_with_mortgage_rates.csv", index=False)
    sold_validation.to_csv(OUTPUT_DIR / "sold_mortgage_merge_validation.csv", index=False)
    listed_validation.to_csv(OUTPUT_DIR / "listed_mortgage_merge_validation.csv", index=False)

    print("Sold null mortgage-rate rows:", int(sold_with_rates["mortgage30us_monthly_avg"].isna().sum()))
    print("Listed null mortgage-rate rows:", int(listed_with_rates["mortgage30us_monthly_avg"].isna().sum()))
    print("\nSold preview:")
    print(sold_with_rates[["CloseDate", "year_month", "ClosePrice", "mortgage30us_monthly_avg"]].head().to_string(index=False))
    print("\nListed preview:")
    print(listed_with_rates[["ListingContractDate", "year_month", "ListPrice", "mortgage30us_monthly_avg"]].head().to_string(index=False))
    print(f"\nSaved mortgage enrichment outputs to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
