# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.healthcare127.blob.core.windows.net",
  "rSAZRaOcvQPqPQYT6z2WK+eseG2mQZnpUM5WFGINubY/MhYS0vd87t+rBwSNmYUqMLpNkEmfwsHP+AStAvkLew=="
)


# COMMAND ----------

delta_path = "wasbs://healthcare127@healthcare127.blob.core.windows.net/bronze_delta/full_healthcare_data"

# COMMAND ----------

df_trans = spark.read.format("delta").load(delta_path)

# COMMAND ----------

display(df_trans)

# COMMAND ----------

# Step 2: Parse Date Columns
from pyspark.sql.functions import to_date, col, when
import pandas as pd

date_cols = ["visit_date", "test_date", "prescribed_on"]
for d in date_cols:
    if d in df_trans.columns:
        df = df_trans.withColumn(d, to_date(col(d), "yyyy-MM-dd"))

# Step 3: Remove Rows with Missing Critical Fields
critical_fields = ["patient_id", "visit_id", "hospital_id"]
df = df.dropna(subset=critical_fields)

# Step 4: Fill Missing Values for Non-Critical Fields
df = df.fillna({
    "lab_result_id": "unknown",
    "test_type": "Not Specified",
    "result_value": 0.0,
    "drug_name": "Not Prescribed",
    "dosage_mg": 0.0,
    "prescription_id": "unknown"
})

# Step 5: Ensure Numeric Fields are Correctly Typed
from pyspark.sql.types import DoubleType, IntegerType

numeric_fields = {
    "treatment_cost": DoubleType(),
    "result_value": DoubleType(),
    "dosage_mg": DoubleType(),
    "bed_capacity": IntegerType()
}


for field, dtype in numeric_fields.items():
    if field in df.columns:
        df = df.withColumn(field, col(field).cast(dtype))



# Fill null test_date and prescribed_on with visit_date
df = df.withColumn("test_date", when(col("test_date").isNull(), col("visit_date")).otherwise(col("test_date")))
df = df.withColumn("prescribed_on", when(col("prescribed_on").isNull(), col("visit_date")).otherwise(col("prescribed_on")))


# COMMAND ----------

display(df)

# COMMAND ----------

silver_path = "wasbs://healthcare127@healthcare127.blob.core.windows.net/silver_delta/full_healthcare_data"

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)
