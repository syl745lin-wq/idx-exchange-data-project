# IDX Real Estate Data Analysis

# Week 1-2

This script performs an initial data exploration and structure check on a real estate dataset (listed_all1.csv) using pandas.

## Step-by-step explanation：

### Load the dataset

Reads the CSV file into a pandas DataFrame called listed.
listed = pd.read_csv("output/listed_all1.csv")
sold = pd.read_csv("output/sold_all.csv")

### Basic dataset overview

Prints the number of rows and columns using .shape
Displays data types of each column using .dtypes

#### DATASET UNDERSTANDING: LISTED

Number of rows and columns:
(756095, 84)
Column data types:

|                           | Column                       | Data Type |
| ------------------------: | :--------------------------- | :-------- |
|                         0 | OriginalListPrice            | float64   |
|                         1 | ListingKey                   | int64     |
|                         2 | ListAgentEmail               | str       |
|                         3 | CloseDate                    | str       |
|                         4 | ClosePrice                   | float64   |
|                         5 | ListAgentFirstName           | str       |
|                         6 | ListAgentLastName            | str       |
|                         7 | Latitude                     | float64   |
|                         8 | Longitude                    | float64   |
|                         9 | UnparsedAddress              | str       |
|                        10 | PropertyType                 | str       |
|                        11 | LivingArea                   | float64   |
|                        12 | ListPrice                    | float64   |
|                        13 | DaysOnMarket                 | int64     |
|                        14 | ListOfficeName               | str       |
|                        15 | BuyerOfficeName              | str       |
|                        16 | CoListOfficeName             | str       |
|                        17 | ListAgentFullName            | str       |
|                        18 | CoListAgentFirstName         | str       |
|                        19 | CoListAgentLastName          | str       |
|                        20 | BuyerAgentMlsId              | str       |
|                        21 | BuyerAgentFirstName          | str       |
|                        22 | BuyerAgentLastName           | str       |
|                        23 | FireplacesTotal              | float64   |
|                        24 | AssociationFeeFrequency      | str       |
|                        25 | AboveGradeFinishedArea       | float64   |
|                        26 | ListingKeyNumeric            | int64     |
|                        27 | MLSAreaMajor                 | str       |
|                        28 | TaxAnnualAmount              | float64   |
|                        29 | CountyOrParish               | str       |
|                        30 | PropertyType.1               | str       |
|                        31 | MlsStatus                    | str       |
|                        32 | ElementarySchool             | str       |
|                        33 | ListAgentFirstName.1         | str       |
|                        34 | AttachedGarageYN             | object    |
|                        35 | ParkingTotal                 | float64   |
|                        36 | BuilderName                  | str       |
|                        37 | PropertySubType              | str       |
|                        38 | LotSizeAcres                 | float64   |
|                        39 | SubdivisionName              | str       |
|                        40 | BuyerOfficeAOR               | str       |
|                        41 | YearBuilt                    | float64   |
|                        42 | DaysOnMarket.1               | int64     |
|                        43 | StreetNumberNumeric          | float64   |
|                        44 | LivingArea.1                 | float64   |
|                        45 | ListingId                    | str       |
|                        46 | BathroomsTotalInteger        | float64   |
|                        47 | City                         | str       |
|                        48 | TaxYear                      | float64   |
|                        49 | BuildingAreaTotal            | float64   |
|                        50 | BedroomsTotal                | float64   |
|                        51 | ContractStatusChangeDate     | str       |
|                        52 | Longitude.1                  | float64   |
|                        53 | ElementarySchoolDistrict     | float64   |
|                        54 | CoBuyerAgentFirstName        | str       |
|                        55 | PurchaseContractDate         | str       |
|                        56 | ListingContractDate          | str       |
|                        57 | BelowGradeFinishedArea       | float64   |
|                        58 | BusinessType                 | str       |
|                        59 | Latitude.1                   | float64   |
|                        60 | ListPrice.1                  | float64   |
|                        61 | StateOrProvince              | str       |
|                        62 | CoveredSpaces                | float64   |
|                        63 | MiddleOrJuniorSchool         | str       |
|                        64 | FireplaceYN                  | object    |
|                        65 | Stories                      | float64   |
|                        66 | HighSchool                   | str       |
|                        67 | Levels                       | str       |
|                        68 | ListAgentLastName.1          | str       |
|                        69 | CloseDate.1                  | str       |
|                        70 | LotSizeDimensions            | str       |
|                        71 | LotSizeArea                  | float64   |
|                        72 | MainLevelBedrooms            | float64   |
|                        73 | NewConstructionYN            | object    |
|                        74 | GarageSpaces                 | float64   |
|                        75 | HighSchoolDistrict           | str       |
|                        76 | PostalCode                   | str       |
|                        77 | BuyerOfficeName.1            | str       |
|                        78 | AssociationFee               | float64   |
|                        79 | LotSizeSquareFeet            | float64   |
|                        80 | MiddleOrJuniorSchoolDistrict | float64   |
|                        81 | UnparsedAddress.1            | str       |
|                        82 | BuyerAgencyCompensationType  | str       |
|                        83 | BuyerAgencyCompensation      | float64   |
| Length: 84, dtype: object |                              |           |

#### DATASET UNDERSTANDING: SOLD

Number of rows and columns:
(492876, 84)

|      | Column                       | Data Type |
| ---: | :--------------------------- | :-------- |
|    0 | BuyerAgentAOR                | str       |
|    1 | ListAgentAOR                 | str       |
|    2 | Flooring                     | str       |
|    3 | ViewYN                       | object    |
|    4 | WaterfrontYN                 | object    |
|    5 | BasementYN                   | object    |
|    6 | PoolPrivateYN                | object    |
|    7 | OriginalListPrice            | float64   |
|    8 | ListingKey                   | int64     |
|    9 | ListAgentEmail               | str       |
|   10 | CloseDate                    | str       |
|   11 | ClosePrice                   | float64   |
|   12 | ListAgentFirstName           | str       |
|   13 | ListAgentLastName            | str       |
|   14 | Latitude                     | float64   |
|   15 | Longitude                    | float64   |
|   16 | UnparsedAddress              | str       |
|   17 | PropertyType                 | str       |
|   18 | LivingArea                   | float64   |
|   19 | ListPrice                    | float64   |
|   20 | DaysOnMarket                 | int64     |
|   21 | ListOfficeName               | str       |
|   22 | BuyerOfficeName              | str       |
|   23 | CoListOfficeName             | str       |
|   24 | ListAgentFullName            | str       |
|   25 | CoListAgentFirstName         | str       |
|   26 | CoListAgentLastName          | str       |
|   27 | BuyerAgentMlsId              | str       |
|   28 | BuyerAgentFirstName          | str       |
|   29 | BuyerAgentLastName           | str       |
|   30 | FireplacesTotal              | float64   |
|   31 | AssociationFeeFrequency      | str       |
|   32 | AboveGradeFinishedArea       | float64   |
|   33 | ListingKeyNumeric            | int64     |
|   34 | MLSAreaMajor                 | str       |
|   35 | TaxAnnualAmount              | float64   |
|   36 | CountyOrParish               | str       |
|   37 | MlsStatus                    | str       |
|   38 | ElementarySchool             | str       |
|   39 | AttachedGarageYN             | object    |
|   40 | ParkingTotal                 | float64   |
|   41 | BuilderName                  | str       |
|   42 | PropertySubType              | str       |
|   43 | LotSizeAcres                 | float64   |
|   44 | SubdivisionName              | str       |
|   45 | BuyerOfficeAOR               | str       |
|   46 | YearBuilt                    | float64   |
|   47 | StreetNumberNumeric          | float64   |
|   48 | ListingId                    | str       |
|   49 | BathroomsTotalInteger        | float64   |
|   50 | City                         | str       |
|   51 | TaxYear                      | float64   |
|   52 | BuildingAreaTotal            | float64   |
|   53 | BedroomsTotal                | float64   |
|   54 | ContractStatusChangeDate     | str       |
|   55 | ElementarySchoolDistrict     | float64   |
|   56 | CoBuyerAgentFirstName        | str       |
|   57 | PurchaseContractDate         | str       |
|   58 | ListingContractDate          | str       |
|   59 | BelowGradeFinishedArea       | float64   |
|   60 | BusinessType                 | str       |
|   61 | StateOrProvince              | str       |
|   62 | CoveredSpaces                | float64   |
|   63 | MiddleOrJuniorSchool         | str       |
|   64 | FireplaceYN                  | object    |
|   65 | Stories                      | float64   |
|   66 | HighSchool                   | str       |
|   67 | Levels                       | str       |
|   68 | LotSizeDimensions            | str       |
|   69 | LotSizeArea                  | float64   |
|   70 | MainLevelBedrooms            | float64   |
|   71 | NewConstructionYN            | object    |
|   72 | GarageSpaces                 | float64   |
|   73 | HighSchoolDistrict           | str       |
|   74 | PostalCode                   | str       |
|   75 | AssociationFee               | float64   |
|   76 | LotSizeSquareFeet            | float64   |
|   77 | MiddleOrJuniorSchoolDistrict | float64   |
|   78 | latfilled                    | object    |
|   79 | lonfilled                    | object    |
|   80 | OriginatingSystemName        | str       |
|   81 | OriginatingSystemSubName     | str       |
|   82 | BuyerAgencyCompensationType  | str       |
|   83 | BuyerAgencyCompensation      | float64   |


### Missing data analysis

Calculates:
Total missing values per column
Percentage of missing values per column
Sorts columns by missing percentage (descending)
Identifies high-missing columns (>90%)

#### DATASET UNDERSTANDING: LISTED

High-missing columns:

|      | index                        | missing_count | missing_percent |
| ---: | :--------------------------- | ------------: | --------------: |
|    0 | FireplacesTotal              |        756095 |             100 |
|    1 | ElementarySchoolDistrict     |        756095 |             100 |
|    2 | MiddleOrJuniorSchoolDistrict |        756095 |             100 |
|    3 | CoveredSpaces                |        756095 |             100 |
|    4 | AboveGradeFinishedArea       |        756095 |             100 |
|    5 | TaxYear                      |        755337 |            99.9 |
|    6 | BelowGradeFinishedArea       |        753082 |            99.6 |
|    7 | BusinessType                 |        750775 |            99.3 |
|    8 | TaxAnnualAmount              |        750680 |           99.28 |
|    9 | CoBuyerAgentFirstName        |        738429 |           97.66 |
|   10 | BuilderName                  |        730052 |           96.56 |
|   11 | LotSizeDimensions            |        713137 |           94.32 |
|   12 | ElementarySchool             |        685803 |            90.7 |
|   13 | MiddleOrJuniorSchool         |        685725 |           90.69 |
|   14 | HighSchool                   |        663682 |           87.78 |
|   15 | BuildingAreaTotal            |        635187 |           84.01 |
|   16 | BuyerAgencyCompensation      |        632576 |           83.66 |
|   17 | BuyerAgencyCompensationType  |        632557 |           83.66 |
|   18 | CoListAgentFirstName         |        605576 |           80.09 |
|   19 | CoListAgentLastName          |        605154 |           80.04 |
|   20 | CoListOfficeName             |        605012 |           80.02 |
|   21 | ClosePrice                   |        546939 |           72.34 |
|   22 | BuyerOfficeAOR               |        546499 |           72.28 |
|   23 | AssociationFeeFrequency      |        537858 |           71.14 |
|   24 | BuyerOfficeName              |        536297 |           70.93 |
|   25 | BuyerOfficeName.1            |        536297 |           70.93 |
|   26 | BuyerAgentFirstName          |        535034 |           70.76 |
|   27 | BuyerAgentMlsId              |        534858 |           70.74 |
|   28 | BuyerAgentLastName           |        534180 |           70.65 |
|   29 | CloseDate.1                  |        527237 |           69.73 |
|   30 | CloseDate                    |        527237 |           69.73 |
|   31 | SubdivisionName              |        503357 |           66.57 |
|   32 | PurchaseContractDate         |        428286 |           56.64 |
|   33 | MainLevelBedrooms            |        405969 |           53.69 |
|   34 | HighSchoolDistrict           |        300430 |           39.73 |
|   35 | AttachedGarageYN             |        257644 |           34.08 |
|   36 | AssociationFee               |        243704 |           32.23 |
|   37 | Stories                      |        196013 |           25.92 |
|   38 | Levels                       |        145295 |           19.22 |
|   39 | GarageSpaces                 |        143505 |           18.98 |
|   40 | FireplaceYN                  |        108641 |           14.37 |
|   41 | NewConstructionYN            |        101804 |           13.46 |
|   42 | LivingArea.1                 |         94369 |           12.48 |
|   43 | LivingArea                   |         94369 |           12.48 |
|   44 | PropertySubType              |         92310 |           12.21 |
|   45 | BedroomsTotal                |         91349 |           12.08 |
|   46 | MLSAreaMajor                 |         85071 |           11.25 |
|   47 | Latitude.1                   |         83686 |           11.07 |
|   48 | Latitude                     |         83686 |           11.07 |
|   49 | Longitude.1                  |         83071 |           10.99 |
|   50 | Longitude                    |         83071 |           10.99 |
|   51 | ListAgentEmail               |         72627 |            9.61 |
|   52 | LotSizeAcres                 |         72154 |            9.54 |
|   53 | LotSizeSquareFeet            |         69775 |            9.23 |
|   54 | LotSizeArea                  |         68822 |             9.1 |
|   55 | BathroomsTotalInteger        |         61944 |            8.19 |
|   56 | YearBuilt                    |         61491 |            8.13 |
|   57 | ParkingTotal                 |         46414 |            6.14 |
|   58 | ContractStatusChangeDate     |          6099 |            0.81 |
|   59 | ListAgentFirstName           |          4424 |            0.59 |
|   60 | ListAgentFirstName.1         |          4424 |            0.59 |
|   61 | StreetNumberNumeric          |          3361 |            0.44 |
|   62 | OriginalListPrice            |          2932 |            0.39 |
|   63 | UnparsedAddress.1            |          1927 |            0.25 |
|   64 | UnparsedAddress              |          1927 |            0.25 |
|   65 | ListPrice                    |          1881 |            0.25 |
|   66 | ListPrice.1                  |          1881 |            0.25 |
|   67 | City                         |           841 |            0.11 |
|   68 | PostalCode                   |           184 |            0.02 |
|   69 | ListAgentFullName            |           186 |            0.02 |
|   70 | StateOrProvince              |            54 |            0.01 |
|   71 | ListAgentLastName            |            54 |            0.01 |
|   72 | ListAgentLastName.1          |            54 |            0.01 |
|   73 | PropertyType                 |             0 |               0 |
|   74 | DaysOnMarket                 |             0 |               0 |
|   75 | ListingKeyNumeric            |             0 |               0 |
|   76 | ListOfficeName               |             0 |               0 |
|   77 | CountyOrParish               |             1 |               0 |
|   78 | PropertyType.1               |             0 |               0 |
|   79 | MlsStatus                    |             0 |               0 |
|   80 | ListingKey                   |             0 |               0 |
|   81 | ListingId                    |             0 |               0 |
|   82 | ListingContractDate          |             0 |               0 |
|   83 | DaysOnMarket.1               |             0 |               0 |

#### DATASET UNDERSTANDING: SOLD

High-missing columns:

|      | index                        | missing_count | missing_percent |
| ---: | :--------------------------- | ------------: | --------------: |
|    0 | ElementarySchoolDistrict     |        492876 |             100 |
|    1 | CoveredSpaces                |        492876 |             100 |
|    2 | FireplacesTotal              |        492876 |             100 |
|    3 | AboveGradeFinishedArea       |        492876 |             100 |
|    4 | MiddleOrJuniorSchoolDistrict |        492876 |             100 |
|    5 | WaterfrontYN                 |        492617 |         99.9475 |
|    6 | TaxYear                      |        492580 |         99.9399 |
|    7 | BusinessType                 |        491564 |         99.7338 |
|    8 | TaxAnnualAmount              |        490810 |         99.5808 |
|    9 | BelowGradeFinishedArea       |        490734 |         99.5654 |
|   10 | BasementYN                   |        484711 |         98.3434 |
|   11 | BuilderName                  |        474268 |         96.2246 |
|   12 | LotSizeDimensions            |        467006 |         94.7512 |
|   13 | CoBuyerAgentFirstName        |        452880 |         91.8852 |
|   14 | BuyerAgencyCompensationType  |        448374 |          90.971 |
|   15 | BuyerAgencyCompensation      |        448348 |         90.9657 |
|   16 | OriginatingSystemName        |        440864 |         89.4472 |
|   17 | OriginatingSystemSubName     |        440864 |         89.4472 |
|   18 | ElementarySchool             |        437891 |         88.8441 |
|   19 | MiddleOrJuniorSchool         |        437463 |         88.7572 |
|   20 | BuildingAreaTotal            |        429254 |         87.0917 |
|   21 | HighSchool                   |        420709 |          85.358 |
|   22 | lonfilled                    |        397083 |         80.5645 |
|   23 | latfilled                    |        397083 |         80.5645 |
|   24 | CoListAgentFirstName         |        390871 |         79.3041 |
|   25 | CoListAgentLastName          |        390660 |         79.2613 |
|   26 | CoListOfficeName             |        386248 |         78.3662 |
|   27 | AssociationFeeFrequency      |        347752 |         70.5557 |
|   28 | SubdivisionName              |        318573 |         64.6355 |
|   29 | MainLevelBedrooms            |        231287 |          46.926 |
|   30 | Flooring                     |        202523 |         41.0901 |
|   31 | HighSchoolDistrict           |        164056 |         33.2855 |
|   32 | AssociationFee               |        153633 |         31.1707 |
|   33 | AttachedGarageYN             |        136176 |         27.6289 |
|   34 | Stories                      |         98487 |         19.9821 |
|   35 | Levels                       |         67642 |         13.7239 |
|   36 | GarageSpaces                 |         62810 |         12.7436 |
|   37 | PoolPrivateYN                |         61690 |         12.5163 |
|   38 | NewConstructionYN            |         58295 |         11.8275 |
|   39 | MLSAreaMajor                 |         54257 |         11.0082 |
|   40 | ViewYN                       |         49188 |         9.97979 |
|   41 | BuyerAgentAOR                |         47624 |         9.66247 |
|   42 | LotSizeAcres                 |         46507 |         9.43584 |
|   43 | LotSizeSquareFeet            |         45339 |         9.19887 |
|   44 | LotSizeArea                  |         44959 |         9.12177 |
|   45 | ListAgentAOR                 |         44738 |         9.07693 |
|   46 | FireplaceYN                  |         42460 |         8.61474 |
|   47 | PropertySubType              |         37395 |          7.5871 |
|   48 | ListAgentEmail               |         35210 |         7.14378 |
|   49 | LivingArea                   |         34265 |         6.95205 |
|   50 | BedroomsTotal                |         32282 |         6.54972 |
|   51 | BathroomsTotalInteger        |         22403 |         4.54536 |
|   52 | BuyerOfficeAOR               |         20854 |         4.23108 |
|   53 | YearBuilt                    |         20556 |         4.17062 |
|   54 | ParkingTotal                 |         15395 |          3.1235 |
|   55 | Latitude                     |         13861 |         2.81227 |
|   56 | Longitude                    |         13748 |         2.78934 |
|   57 | PurchaseContractDate         |         10032 |          2.0354 |
|   58 | BuyerOfficeName              |          6118 |         1.24129 |
|   59 | ListAgentFirstName           |          2883 |        0.584934 |
|   60 | BuyerAgentFirstName          |          2169 |         0.44007 |
|   61 | BuyerAgentMlsId              |          1512 |        0.306771 |
|   62 | OriginalListPrice            |          1413 |        0.286685 |
|   63 | StreetNumberNumeric          |          1220 |        0.247527 |
|   64 | UnparsedAddress              |           757 |        0.153588 |
|   65 | ListPrice                    |           743 |        0.150748 |
|   66 | ContractStatusChangeDate     |           712 |        0.144458 |
|   67 | City                         |           362 |       0.0734465 |
|   68 | BuyerAgentLastName           |           270 |       0.0547805 |
|   69 | PostalCode                   |           149 |       0.0302307 |
|   70 | ListAgentFullName            |           123 |       0.0249556 |
|   71 | ListingContractDate          |            63 |       0.0127821 |
|   72 | ListAgentLastName            |            51 |       0.0103474 |
|   73 | ClosePrice                   |             4 |     0.000811563 |
|   74 | CloseDate                    |             0 |               0 |
|   75 | ListingKey                   |             0 |               0 |
|   76 | ListingId                    |             0 |               0 |
|   77 | PropertyType                 |             0 |               0 |
|   78 | ListOfficeName               |             0 |               0 |
|   79 | StateOrProvince              |             0 |               0 |
|   80 | ListingKeyNumeric            |             0 |               0 |
|   81 | CountyOrParish               |             0 |               0 |
|   82 | MlsStatus                    |             0 |               0 |
|   83 | DaysOnMarket                 |             0 |               0 |

### Feature categorization

Defines two groups of columns:
Market analysis fields (useful for analysis, e.g., price, location, size)
Metadata fields (administrative or agent-related info)

### Column validation

Checks which of the predefined fields actually exist in the dataset
Prints:
Available market analysis fields
Available metadata fields

#### DATASET UNDERSTANDING: LISTED

Market analysis fields found in dataset:
['ListingId', 'PropertyType', 'PropertySubType', 'City', 'CountyOrParish', 'PostalCode', 'CloseDate', 'ListingContractDate', 'ClosePrice', 'ListPrice', 'OriginalListPrice', 'LivingArea', 'LotSizeAcres', 'BedroomsTotal', 'BathroomsTotalInteger', 'DaysOnMarket', 'YearBuilt']
Metadata fields found in dataset:
['ListingKey', 'ListingKeyNumeric', 'ListAgentEmail', 'ListAgentFirstName', 'ListAgentLastName', 'ListAgentFullName', 'BuyerAgentFirstName', 'BuyerAgentLastName', 'BuyerOfficeName', 'ListOfficeName']

#### DATASET UNDERSTANDING: SOLD

Market analysis fields found in dataset:
['ListingId', 'PropertyType', 'PropertySubType', 'City', 'CountyOrParish', 'PostalCode', 'CloseDate', 'ListingContractDate', 'ClosePrice', 'ListPrice', 'OriginalListPrice', 'LivingArea', 'LotSizeAcres', 'BedroomsTotal', 'BathroomsTotalInteger', 'DaysOnMarket', 'YearBuilt']
Metadata fields found in dataset:
['ListingKey', 'ListingKeyNumeric', 'ListAgentEmail', 'ListAgentFirstName', 'ListAgentLastName', 'ListAgentFullName', 'BuyerAgentFirstName', 'BuyerAgentLastName', 'BuyerOfficeName', 'ListOfficeName']

# Week 2-3
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
 
# Week 4-5
## General Scope
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

- Sold: 330,386 before cleaning and 330,243 after cleaning.
- Listed: 478,539 before cleaning and 478,205 after cleaning.

## Date Consistency Results

### Sold

| flag_name                 | flag_count |
| :------------------------ | ---------: |
| listing_after_close_flag  |         48 |
| purchase_after_close_flag |        192 |
| negative_timeline_flag    |        223 |

### Listed

| flag_name                 | flag_count |
| :------------------------ | ---------: |
| listing_after_close_flag  |         66 |
| purchase_after_close_flag |        241 |
| negative_timeline_flag    |        235 |

## Geographic Quality Results

### Sold

| flag_name                   | flag_count |
| :-------------------------- | ---------: |
| missing_coordinate_flag     |      11452 |
| zero_coordinate_flag        |         18 |
| longitude_positive_flag     |         25 |
| out_of_state_flag           |         25 |
| implausible_coordinate_flag |         67 |

### Listed

| flag_name                   | flag_count |
| :-------------------------- | ---------: |
| missing_coordinate_flag     |      61222 |
| zero_coordinate_flag        |         56 |
| longitude_positive_flag     |         67 |
| out_of_state_flag           |        137 |
| implausible_coordinate_flag |        251 |

## Deliverables

- `output/week4_5/sold/sold_analysis_ready_week45.csv`
- `output/week4_5/listed/listed_analysis_ready_week45.csv`
- Row count summaries, dtype confirmations, missing summaries, date flag summaries, geographic quality summaries, and transformation logs for both datasets.
