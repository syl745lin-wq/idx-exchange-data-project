# IDX Real Estate Data Analysis

This project completes the Week 2-3 requirements for MLS sold and listed real-estate data analysis.

## Week 2-3 Objectives
- Inspect dataset structure
- Validate row counts before and after concatenation
- Review column data types
- Identify high-missing columns
- Separate market-analysis fields from metadata fields
- Filter datasets to `PropertyType == 'Residential'`
- Produce missing-value reports and numeric distribution summaries
- Generate histograms and boxplots for key numeric fields
- Identify extreme outliers for later cleaning
- Enrich sold and listed datasets with monthly mortgage rates from FRED

## Project Files
- [sold_analysis.py](/Users/siyulin/Desktop/Internship/idx-project/sold_analysis.py): builds the combined sold dataset from raw monthly files, validates append counts, filters residential properties, and exports Week 2-3 reports.
- [listed_analysis.py](/Users/siyulin/Desktop/Internship/idx-project/listed_analysis.py): builds the combined listed dataset from raw monthly files, validates append counts, filters residential properties, and exports Week 2-3 reports.
- [week23_common.py](/Users/siyulin/Desktop/Internship/idx-project/week23_common.py): shared Week 2-3 logic for loading files, dataset understanding, missing-value analysis, filtering, summaries, and plots.
- [mortgage_rate_enrichment.py](/Users/siyulin/Desktop/Internship/idx-project/mortgage_rate_enrichment.py): fetches `MORTGAGE30US` from FRED, converts weekly rates to monthly averages, and merges them onto the residential sold and listed datasets.

## Data Inputs
Raw input files are expected in:
- [data/raw](/Users/siyulin/Desktop/Internship/idx-project/data/raw)

Main patterns used by the scripts:
- `CRMLSSold*.csv`
- `CRMLSListing*.csv`

## How To Run
From the project directory:

```bash
cd /Users/siyulin/Desktop/Internship/idx-project
python3 sold_analysis.py
python3 listed_analysis.py
python3 mortgage_rate_enrichment.py
```

## Week 2-3 Outputs
All generated outputs are saved under:
- [output/week2_3](/Users/siyulin/Desktop/Internship/idx-project/output/week2_3)

### Sold outputs
Saved in:
- [output/week2_3/sold](/Users/siyulin/Desktop/Internship/idx-project/output/week2_3/sold)

Key files:
- `sold_combined_week23.csv`
- `sold_residential_week23.csv`
- `sold_row_counts.csv`
- `sold_property_type_before_filter.csv`
- `sold_property_type_after_filter.csv`
- `sold_filter_validation.csv`
- `sold_dtypes.csv`
- `sold_missing_value_report.csv`
- `sold_high_missing_gt90.csv`
- `sold_column_retention_report.csv`
- `sold_numeric_distribution_summary.csv`
- `sold_outlier_summary.csv`
- `sold_intern_questions.csv`
- `plots/` with histograms and boxplots for the required numeric fields

### Listed outputs
Saved in:
- [output/week2_3/listed](/Users/siyulin/Desktop/Internship/idx-project/output/week2_3/listed)

Key files:
- `listed_combined_week23.csv`
- `listed_residential_week23.csv`
- `listed_row_counts.csv`
- `listed_property_type_before_filter.csv`
- `listed_property_type_after_filter.csv`
- `listed_filter_validation.csv`
- `listed_dtypes.csv`
- `listed_missing_value_report.csv`
- `listed_high_missing_gt90.csv`
- `listed_column_retention_report.csv`
- `listed_numeric_distribution_summary.csv`
- `listed_outlier_summary.csv`
- `listed_intern_questions.csv`
- `plots/` with histograms and boxplots for the required numeric fields

### Mortgage enrichment outputs
Saved in:
- [output/week2_3/mortgage_enrichment](/Users/siyulin/Desktop/Internship/idx-project/output/week2_3/mortgage_enrichment)

Key files:
- `fred_mortgage30us_weekly.csv`
- `fred_mortgage30us_monthly.csv`
- `sold_residential_with_mortgage_rates.csv`
- `listed_residential_with_mortgage_rates.csv`
- `sold_mortgage_merge_validation.csv`
- `listed_mortgage_merge_validation.csv`

## Validation Covered
The scripts include the Week 2-3 validation steps requested in class:
- Row count for each raw file before append
- Combined row count after concatenation
- PropertyType frequency table before filtering
- Row count before and after `PropertyType == 'Residential'`
- Missing-value report with columns above 90% null flagged
- Numeric distribution summary with percentiles
- Merge validation confirming no null mortgage-rate values after enrichment

## Skills Demonstrated
- Dataset validation and quality checks
- Exploratory data analysis (EDA)
- Property type filtering for MLS datasets
- Missing-value profiling and retention decisions
- Numeric distribution review and outlier screening
- Time-series enrichment using external FRED mortgage-rate data

## Note
Raw MLS data may be confidential. If this repository is shared publicly, upload code only and exclude sensitive raw or processed CSV files unless permission has been granted.
