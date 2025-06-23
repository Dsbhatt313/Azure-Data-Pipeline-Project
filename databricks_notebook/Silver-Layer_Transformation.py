# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np

# COMMAND ----------

# MAGIC %md
# MAGIC #Sliver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Access using App

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.storagedatalake18.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.storagedatalake18.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.storagedatalake18.dfs.core.windows.net", "459f5304-db11-4cf2-9778-d1d330782161")
spark.conf.set("fs.azure.account.oauth2.client.secret.storagedatalake18.dfs.core.windows.net", "lhs8Q~5R7MG1dzr2Mgh_1rRNx6-_Wffdcs27qaAM")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.storagedatalake18.dfs.core.windows.net", "https://login.microsoftonline.com/89bdf7d8-65c4-4a41-b205-23c91bfddf82/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Data

# COMMAND ----------

df_cal = spark.read.option("header", True).option("inferSchema", True).csv("abfss://bronze@storagedatalake18.dfs.core.windows.net/Calendar")

# COMMAND ----------

df_cus = spark.read.option("header", True).option("inferSchema", True).csv("abfss://bronze@storagedatalake18.dfs.core.windows.net/Customers")

# COMMAND ----------

df_procat = spark.read.option("header", True).option("inferSchema", True).csv("abfss://bronze@storagedatalake18.dfs.core.windows.net/Product_Categories")

# COMMAND ----------

df_prosub = spark.read.option("header", True).option("inferSchema", True).csv("abfss://bronze@storagedatalake18.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

df_pro = spark.read.option("header", True).option("inferSchema", True).csv("abfss://bronze@storagedatalake18.dfs.core.windows.net/Products")

# COMMAND ----------

df_ret = spark.read.option("header", True).option("inferSchema", True).csv("abfss://bronze@storagedatalake18.dfs.core.windows.net/Returns")

# COMMAND ----------

df_sales = spark.read.option("header", True).option("inferSchema", True).csv("abfss://bronze@storagedatalake18.dfs.core.windows.net/Sales*")

# COMMAND ----------

df_terr = spark.read.option("header", True).option("inferSchema", True).csv("abfss://bronze@storagedatalake18.dfs.core.windows.net/Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calender

# COMMAND ----------

df_cal = df_cal.withColumn('Month', month(col('Date'))).withColumn('Year', year(col('Date')))

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet').mode('append').option('path', "abfss://silver@storagedatalake18.dfs.core.windows.net/Calendar_Transformed").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus = df_cus.withColumn("FullName", concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))
df_cus.display()

# COMMAND ----------

df_cus = df_cus.drop('Prefix', 'FirstName', 'LastName')
df_cus.display()

# COMMAND ----------

df_cus = df_cus.withColumn("Gender", when(col('Gender') == 'M', 0).otherwise(1))
df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet').mode('append').option('path', "abfss://silver@storagedatalake18.dfs.core.windows.net/Customers_Transformed").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Categories

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_procat.write.format('parquet').mode('append').option('path', "abfss://silver@storagedatalake18.dfs.core.windows.net/Product_Categories_Transformed").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Subcategories

# COMMAND ----------

df_prosub.display()

# COMMAND ----------

df_prosub.write.format('parquet').mode('append').option('path', "abfss://silver@storagedatalake18.dfs.core.windows.net/Product_Subcategories_Transformed").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0]).drop('ProductName')
df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductCost', round(col('ProductCost'), 2)).withColumn('ProductPrice', round(col('ProductPrice'), 2))
df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet').mode('append').option('path', "abfss://silver@storagedatalake18.dfs.core.windows.net/Products_Transformed").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Returns

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret = df_ret.withColumn('ReturnDate', date_format(col('ReturnDate'), 'MM/dd/yyyy'))
df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet').mode('append').option('path', "abfss://silver@storagedatalake18.dfs.core.windows.net/Returns_Transformed").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Territories

# COMMAND ----------

df_terr.display()

# COMMAND ----------

df_terr.write.format('parquet').mode('append').option('path', "abfss://silver@storagedatalake18.dfs.core.windows.net/Territories_Transformed").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate', to_timestamp('StockDate'))\
                   .withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))\
                   .withColumn('OrderDate', date_format(col('OrderDate'), 'MM/dd/yyyy'))
df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet').mode('append').option('path', "abfss://silver@storagedatalake18.dfs.core.windows.net/Sales_Transformed").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('TotalOrders')).display()

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_terr.display()

# COMMAND ----------

