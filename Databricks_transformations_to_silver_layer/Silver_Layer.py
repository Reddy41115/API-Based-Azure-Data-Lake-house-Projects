# Databricks notebook source
# MAGIC %md
# MAGIC ### Data access using App ###

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.auth.type.storageaccountadlsreddy.dfs.core.windows.net",
  "OAuth"
)

spark.conf.set(
  "fs.azure.account.oauth.provider.type.storageaccountadlsreddy.dfs.core.windows.net",
  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)

spark.conf.set(
  "fs.azure.account.oauth2.client.id.storageaccountadlsreddy.dfs.core.windows.net",
  "2b217106-7c45-49c0-a485-d8c4e724cf15"
)

spark.conf.set(
  "fs.azure.account.oauth2.client.secret.storageaccountadlsreddy.dfs.core.windows.net",
  "Mcz8Q~qpvDt8uI9eIo5KhIqDxaNQwYHHIyFhnbeZ"
)

spark.conf.set(
  "fs.azure.account.oauth2.client.endpoint.storageaccountadlsreddy.dfs.core.windows.net",
  "https://login.microsoftonline.com/7f2ecc21-51fa-4592-b5c6-b7767a08a37d/oauth2/token"
)


# COMMAND ----------

dbutils.fs.ls("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Load ###

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Calander Data ###

# COMMAND ----------

df_cal = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Calendar")

df_customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Customers")

df_product_categories = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Product_Categories")

df_product_subcategories = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Product_Subcategories")

df_products = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Products")

df_returns = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Returns")

df_sales_2015 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Sales_2015")

df_sales_2016 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Sales_2016")

df_sales_2017 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Sales_2017")

df_territories = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://bronze@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Territories")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations ###

# COMMAND ----------

df_cal = df_cal.withColumn("Month", month(col("Date")))\
    .withColumn("Year", year(col("Date")))
df_cal.show()

# COMMAND ----------

df_cal.write.format("parquet")\
    .mode("append")\
        .option("path", "abfss://silver@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Calendar")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers Transformations ###

# COMMAND ----------

df_customers.show()

# COMMAND ----------

df_customers = df_customers.withColumn("FullName", concat_ws(" ", col("Prefix"),col("FirstName"),col("LastName")))

# COMMAND ----------

df_customers.write.format("parquet")\
    .mode("append")\
        .option("path", "abfss://silver@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Customers")\
            .save()

# COMMAND ----------

df_customers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### AdventureWorks_Product_Categories ###

# COMMAND ----------

df_product_categories.show()

# COMMAND ----------

df_product_categories.write.format("parquet")\
    .mode("append")\
        .option("path","abfss://silver@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Product_Categories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### AdventureWorks_Product_Subcategories ###

# COMMAND ----------

df_product_subcategories.show()

# COMMAND ----------

df_product_subcategories.write.format("parquet")\
    .mode("append")\
        .option("path", "abfss://silver@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Product_Subcategories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### AdventureWorks_Products ###

# COMMAND ----------

display(df_products)

# COMMAND ----------

df_products = df_products.withColumn("ProductSKU", split(col("ProductSKU"),"-")[0])\
    .withColumn("ProductName", split("ProductName"," ")[0])
display(df_products)

# COMMAND ----------

df_products.write.format("parquet")\
    .mode("append")\
        .option("path","abfss://silver@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Products")\
            .save()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ### AdventureWorks_Returns ###

# COMMAND ----------

display(df_returns)

# COMMAND ----------

df_returns.write.format("parquet")\
    .mode("append")\
        .option("path","abfss://silver@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Returns")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### AdventureWorks_Territories ###

# COMMAND ----------

display(df_territories)

# COMMAND ----------

df_territories.write.format("parquet")\
    .mode("append")\
        .option("path","abfss://silver@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Territories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### AdventureWorks_Sales_2015 ###

# COMMAND ----------

display(df_sales_2015)

# COMMAND ----------

df_sales_2015 = df_sales_2015.withColumn("StockDate", to_timestamp("StockDate"))\
    .withColumn("OrderNumber", regexp_replace("OrderNumber","S","T"))\
        .withColumn("multiply", col("TerritoryKey")*col("OrderLineItem"))
display(df_sales_2015)

# COMMAND ----------

df_sales_2015.write.format("parquet")\
    .mode("append")\
        .option("path","abfss://silver@storageaccountadlsreddy.dfs.core.windows.net/AdventureWorks_Sales_2015")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Analysis ###

# COMMAND ----------

df_sales_2015.groupBy("OrderDate").agg(count("OrderNumber").alias("Counts")).display()

# COMMAND ----------

df_product_categories.display()

# COMMAND ----------

display(df_territories)