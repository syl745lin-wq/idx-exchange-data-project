from week23_common import run_week23_analysis


if __name__ == "__main__":
    outputs = run_week23_analysis(
        dataset_name="sold",
        raw_pattern="CRMLSSold*.csv",
        combined_filename="sold_combined_week23.csv",
        filtered_filename="sold_residential_week23.csv",
    )

    print("\nSaved sold Week 2-3 outputs to:")
    print(outputs["dataset_dir"])
