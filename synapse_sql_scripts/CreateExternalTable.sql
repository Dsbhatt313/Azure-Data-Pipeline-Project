CREATE MASTER KEY ENCRYPTION BY PASSWORD ='Rinkiyapapa@1' 


CREATE DATABASE SCOPED CREDENTIAL cred_dhruvansh
WITH IDENTITY = 'Managed Identity'


CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION = 'https://storagedatalake18.dfs.core.windows.net/silver',
    CREDENTIAL = cred_dhruvansh
)


CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://storagedatalake18.dfs.core.windows.net/gold',
    CREDENTIAL = cred_dhruvansh
)


CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


---------------------------------
-- Create External Table extsales
---------------------------------

CREATE EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS SELECT * FROM gold.sales


------------------------------------
-- Create External Table extcalendar
------------------------------------

CREATE EXTERNAL TABLE gold.extcalendar
WITH
(
    LOCATION = 'extcalendar',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS SELECT * FROM gold.calendar


------------------------------------
-- Create External Table extcustomers
------------------------------------

CREATE EXTERNAL TABLE gold.extcustomers
WITH
(
    LOCATION = 'extcustomers',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS SELECT * FROM gold.customers


------------------------------------
-- Create External Table extproduct_categories
------------------------------------

CREATE EXTERNAL TABLE gold.extproduct_categories
WITH
(
    LOCATION = 'extproduct_categories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS SELECT * FROM gold.product_categories


------------------------------------
-- Create External Table extproduct_subcategories
------------------------------------

CREATE EXTERNAL TABLE gold.extproduct_subcategories
WITH
(
    LOCATION = 'extproduct_subcategories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS SELECT * FROM gold.product_subcategories


------------------------------------
-- Create External Table extproducts
------------------------------------

CREATE EXTERNAL TABLE gold.extproducts
WITH
(
    LOCATION = 'extproducts',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS SELECT * FROM gold.products


------------------------------------
-- Create External Table extreturnss
------------------------------------

CREATE EXTERNAL TABLE gold.extreturnss
WITH
(
    LOCATION = 'extreturnss',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS SELECT * FROM gold.returnss


------------------------------------
-- Create External Table extterritories
------------------------------------

CREATE EXTERNAL TABLE gold.extterritories
WITH
(
    LOCATION = 'extterritories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS SELECT * FROM gold.territories