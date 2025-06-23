------------------------
-- Create view calendar
------------------------
CREATE VIEW gold.calendar
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storagedatalake18.dfs.core.windows.net/silver/Calendar_Transformed/',
            FORMAT = 'PARQUET'
        ) AS Query1

------------------------
-- Create view customers
------------------------
CREATE VIEW gold.customers
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storagedatalake18.dfs.core.windows.net/silver/Customers_Transformed/',
            FORMAT = 'PARQUET'
        ) AS Query2

------------------------
-- Create view product categories
------------------------
CREATE VIEW gold.product_categories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storagedatalake18.dfs.core.windows.net/silver/Product_Categories_Transformed/',
            FORMAT = 'PARQUET'
        ) AS Query3

------------------------
-- Create view product_subcategories
------------------------
CREATE VIEW gold.product_subcategories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storagedatalake18.dfs.core.windows.net/silver/Product_Subcategories_Transformed/',
            FORMAT = 'PARQUET'
        ) AS Query4

------------------------
-- Create view products
------------------------
CREATE VIEW gold.products
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storagedatalake18.dfs.core.windows.net/silver/Products_Transformed/',
            FORMAT = 'PARQUET'
        ) AS Query5

------------------------
-- Create view returns
------------------------
CREATE VIEW gold.returnss
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storagedatalake18.dfs.core.windows.net/silver/Returns_Transformed/',
            FORMAT = 'PARQUET'
        ) AS Query6

------------------------
-- Create view sales
------------------------
CREATE VIEW gold.sales
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storagedatalake18.dfs.core.windows.net/silver/Sales_Transformed/',
            FORMAT = 'PARQUET'
        ) AS Query7

------------------------
-- Create view territories
------------------------
CREATE VIEW gold.territories
AS
SELECT 
    * 
FROM 
    OPENROWSET
        (
            BULK 'https://storagedatalake18.dfs.core.windows.net/silver/Territories_Transformed/',
            FORMAT = 'PARQUET'
        ) AS Query8