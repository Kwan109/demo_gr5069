# Databricks notebook source
# MAGIC %md # Workshop - Data Pipeline Practice (Guankun)

# COMMAND ----------

# MAGIC %md Read in Dataset

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date, avg
from pyspark.sql.types import IntegerType

# COMMAND ----------

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header = True)

# COMMAND ----------

display(df_laptimes)

# COMMAND ----------

df_driver = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header = True)
df_driver.count()

# COMMAND ----------

display(df_driver)

# COMMAND ----------

# MAGIC %md ##### Transform Data

# COMMAND ----------

df_driver = df_driver.withColumn("age", datediff(current_date(),df_driver.dob)/365)

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_driver = df_driver.withColumn('age', df_driver['age'].cast(IntegerType()))

# COMMAND ----------

df_lap_drivers = df_driver.select('driverId','driverRef', 'forename', 'surname','nationality','age').join(df_laptimes, on=['driverId'])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md #### Aggregate by Age

# COMMAND ----------

df_lap_drivers = df_lap_drivers.groupby('age').agg(avg('milliseconds'))

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md ## Storing Data in S3

# COMMAND ----------

df_lap_drivers.write.csv('s3://gc-gr5069/processed/laptimes_by.csv')

# COMMAND ----------

