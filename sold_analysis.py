import pandas as pd

sold = pd.read_csv("output/sold_all.csv")

print("DATASET UNDERSTANDING: SOLD")

# 1. Identify number of rows and columns
print("Number of rows and columns:")
print(sold.shape)

# 2. Review column data types
print("Column data types:")
print(sold.dtypes)
dtypes_df = sold.dtypes.reset_index()
dtypes_df.columns = ["Column", "Data Type"]

print(dtypes_df.to_markdown())
# 3. Identify high-missing columns
missing_summary = pd.DataFrame({
    "missing_count": sold.isnull().sum(),
    "missing_percent": (sold.isnull().mean() * 100)
}).sort_values(by="missing_percent", ascending=False)

print("High-missing columns:")
print(missing_summary[missing_summary["missing_percent"] > 90])
print(missing_summary.reset_index().to_markdown())
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
print([col for col in market_analysis_fields if col in sold.columns])

print("Metadata fields found in dataset:")
print([col for col in metadata_fields if col in sold.columns])
