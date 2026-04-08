# IDX Real Estate Data Analysis

This repository contains Python scripts for sold and listed transaction analysis.

## Files
- sold_analysis.py
- listed_analysis.py

## Note
Raw and processed CSV files are excluded from this repository due to confidentiality.

# Week 1-2
This script performs an initial data exploration and structure check on a real estate dataset (listed_all1.csv) using pandas.

## Step-by-step explanation：
### Load the dataset
Reads the CSV file into a pandas DataFrame called listed.
### Basic dataset overview
Prints the number of rows and columns using .shape
Displays data types of each column using .dtypes
### Missing data analysis
Calculates:
Total missing values per column
Percentage of missing values per column
Sorts columns by missing percentage (descending)
Identifies high-missing columns (>90%)
### Feature categorization
Defines two groups of columns:
Market analysis fields (useful for analysis, e.g., price, location, size)
Metadata fields (administrative or agent-related info)
### Column validation
Checks which of the predefined fields actually exist in the dataset
Prints:
Available market analysis fields
Available metadata fields
