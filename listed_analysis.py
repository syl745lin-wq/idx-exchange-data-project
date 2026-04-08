import pandas as pd

listed = pd.read_csv("output/listed_all1.csv")

print("DATASET UNDERSTANDING: LISTED")

# 1. Identify number of rows and columns
print("Number of rows and columns:")
print(listed.shape)

# 2. Review column data types
print("Column data types:")
print(listed.dtypes)

# 3. Identify high-missing columns
missing_summary = pd.DataFrame({
    "missing_count": listed.isnull().sum(),
    "missing_percent": (listed.isnull().mean() * 100).round(2)
}).sort_values(by="missing_percent", ascending=False)

print("High-missing columns:")
print(missing_summary[missing_summary["missing_percent"] > 90])

# 4. Separate market analysis fields from metadata fields
market_analysis_fields = [
    "ListingId",
    "PropertyType",
    "PropertySubType",
    "City",
    "CountyOrParish",
    "PostalCode",
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

metadata_fields = [
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
]

print("Market analysis fields found in dataset:")
print([col for col in market_analysis_fields if col in listed.columns])

print("Metadata fields found in dataset:")
print([col for col in metadata_fields if col in listed.columns])