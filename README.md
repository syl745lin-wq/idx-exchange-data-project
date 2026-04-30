# IDX Real Estate Data Analysis

This project documents a multi-week MLS data workflow for residential real estate analysis. The pipeline starts from raw monthly sold and listed CSV files, filters them to residential records, enriches them with mortgage rate data, and then produces cleaned, analysis-ready datasets for later market analysis.

## Project Structure

- [sold_analysis.py](/Users/siyulin/Desktop/Internship/idx-project/sold_analysis.py)
  Runs the sold-data workflow for Week 2-3 and Week 4-5.
- [listed_analysis.py](/Users/siyulin/Desktop/Internship/idx-project/listed_analysis.py)
  Runs the listed-data workflow for Week 2-3 and Week 4-5.
- [mortgage_rate_enrichment.py](/Users/siyulin/Desktop/Internship/idx-project/mortgage_rate_enrichment.py)
  Enriches residential sold and listed datasets with monthly FRED mortgage rates.
- [output/week2_3](/Users/siyulin/Desktop/Internship/idx-project/output/week2_3)
  Contains dataset structuring, validation, and mortgage enrichment outputs.
- [output/week4_5](/Users/siyulin/Desktop/Internship/idx-project/output/week4_5)
  Contains cleaned, analysis-ready datasets and quality summaries.

## Week 2-3: Dataset Structuring and Validation

The Week 2-3 stage focused on understanding the raw datasets and preparing a reliable residential subset for later analysis.

### Main tasks

- Combine monthly raw sold and listed CSV files
- Confirm row counts before and after concatenation
- Review dataset shape and column data types
- Separate market-analysis fields from metadata fields
- Identify columns with high missing percentages
- Filter to `PropertyType == "Residential"`
- Produce missing-value summaries and high-missing reports
- Generate numeric distribution summaries and outlier summaries

### Main outputs

- [output/week2_3/sold/sold_residential_week23.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week2_3/sold/sold_residential_week23.csv)
- [output/week2_3/listed/listed_residential_week23.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week2_3/listed/listed_residential_week23.csv)
- Missing-value reports, row count reports, property-type summaries, and numeric distribution summaries

## Mortgage Rate Enrichment

After residential filtering, both datasets were enriched with the FRED `MORTGAGE30US` series.

### Process

- Fetch weekly mortgage rate data from FRED
- Convert weekly observations into monthly averages
- Create a `year_month` join key
- Merge rates onto:
  - sold data using `CloseDate`
  - listed data using `ListingContractDate`

### Main outputs

- [output/week2_3/mortgage_enrichment/sold_residential_with_mortgage_rates.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week2_3/mortgage_enrichment/sold_residential_with_mortgage_rates.csv)
- [output/week2_3/mortgage_enrichment/listed_residential_with_mortgage_rates.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week2_3/mortgage_enrichment/listed_residential_with_mortgage_rates.csv)

## Week 4-5: Data Cleaning and Preparation

The Week 4-5 stage transformed the residential mortgage-enriched datasets into analysis-ready sold and listed datasets.

### Cleaning logic

For both `sold` and `listed`, the scripts:

- convert date fields to `datetime`
- convert key numeric fields to numeric types
- normalize blank strings into missing values
- remove unnecessary metadata columns
- add date consistency flags
- add geographic quality flags
- add invalid numeric flags
- remove rows with critical invalid values or missing required fields

### Important business difference

The two datasets are not cleaned with exactly the same rules:

- `sold` is treated as a closed transaction dataset, so `CloseDate` and `ClosePrice` are required
- `listed` is treated as an active/listing dataset, so `CloseDate` and `ClosePrice` are allowed to remain missing when the property has not closed

This distinction is important because it prevents over-cleaning the listed dataset.

### Key Week 4-5 results

- Sold rows before cleaning: `330,386`
- Sold rows after cleaning: `330,243`
- Sold rows removed: `143`

- Listed rows before cleaning: `478,539`
- Listed rows after cleaning: `478,205`
- Listed rows removed: `334`

### Main outputs

- [output/week4_5/sold/sold_analysis_ready_week45.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week4_5/sold/sold_analysis_ready_week45.csv)
- [output/week4_5/listed/listed_analysis_ready_week45.csv](/Users/siyulin/Desktop/Internship/idx-project/output/week4_5/listed/listed_analysis_ready_week45.csv)
- Row count summaries
- Missing summaries
- Data type confirmations
- Invalid numeric summaries
- Date flag summaries
- Geographic quality summaries
- Transformation logs

## Week 5 Summary

The Week 5 work focused on documenting and interpreting the cleaning stage in more detail.

### Main conclusions

- The cleaning stage removed only a small number of rows, which means earlier residential filtering was already stable.
- The most common invalid numeric problem in both datasets was `LivingArea <= 0`.
- The largest geographic issue was missing coordinates, especially in the listed dataset.
- Date-order problems were not immediately removed. Instead, they were preserved as boolean flags for later review.
- The listed dataset intentionally retained large amounts of missing `ClosePrice` and `CloseDate`, because those nulls are business-valid for properties that have not yet closed.

### Supporting report

- [week5_summary_report.md](/Users/siyulin/Desktop/Internship/idx-project/week5_summary_report.md)

## Running the Pipeline

Run the scripts from the project root:

```bash
/usr/local/bin/python3.13 sold_analysis.py
/usr/local/bin/python3.13 listed_analysis.py
/usr/local/bin/python3.13 mortgage_rate_enrichment.py
```

## Deliverables

This repository now contains:

- residential sold and listed datasets
- mortgage-rate enriched datasets
- cleaned analysis-ready datasets
- validation summaries
- missing-value summaries
- date consistency and geographic quality reports
- written weekly reports and presentation materials

## Notes

- The cleaned datasets preserve useful null values when they remain meaningful for analysis.
- Quality flags are used to make suspicious records transparent without automatically dropping all of them.
- The workflow is designed to support later EDA, county-level analysis, time-series analysis, and modeling preparation.
