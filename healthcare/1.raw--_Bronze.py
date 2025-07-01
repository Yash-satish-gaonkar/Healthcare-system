# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.healthcare127.blob.core.windows.net",
  "rSAZRaOcvQPqPQYT6z2WK+eseG2mQZnpUM5WFGINubY/MhYS0vd87t+rBwSNmYUqMLpNkEmfwsHP+AStAvkLew=="
)


# COMMAND ----------

csv_path = "wasbs://healthcare127@healthcare127.blob.core.windows.net/data/full_healthcare_data.csv"

df_bronze = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path)


# COMMAND ----------

delta_path = "wasbs://healthcare127@healthcare127.blob.core.windows.net/bronze_delta/full_healthcare_data"

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .save(delta_path)

display(df_bronze)
