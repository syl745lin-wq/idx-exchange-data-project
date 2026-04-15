from week23_common import run_week23_analysis


if __name__ == "__main__":
    outputs = run_week23_analysis(
        dataset_name="listed",
        raw_pattern="CRMLSListing*.csv",
        combined_filename="listed_combined_week23.csv",
        filtered_filename="listed_residential_week23.csv",
    )

    print("\nSaved listed Week 2-3 outputs to:")
    print(outputs["dataset_dir"])
