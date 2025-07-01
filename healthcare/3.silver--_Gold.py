# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.healthcare127.blob.core.windows.net",
  "rSAZRaOcvQPqPQYT6z2WK+eseG2mQZnpUM5WFGINubY/MhYS0vd87t+rBwSNmYUqMLpNkEmfwsHP+AStAvkLew=="
)

# COMMAND ----------

silver_path = "wasbs://healthcare127@healthcare127.blob.core.windows.net/silver_delta/full_healthcare_data"

df_agg = spark.read.format("delta").load(silver_path)
display(df_agg)

# COMMAND ----------

from pyspark.sql.functions import col, year, month, dayofmonth, to_date
from pyspark.sql import functions as F

# ✅ DIM: PATIENT
dim_patient = df_agg.select("patient_id").distinct()

# ✅ DIM: HOSPITAL
dim_hospital = df_agg.select("hospital_id", "hospital_name", "city", "region", "bed_capacity").dropDuplicates().orderBy(F.asc("hospital_id"))

# ✅ DIM: DOCTOR
dim_doctor = df_agg.select("doctor_id").distinct()

# ✅ DIM: DATE (from all date columns)
dim_date = df_agg.select("visit_date").distinct().withColumnRenamed("visit_date", "date") \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date"))

# ✅ FACT: PATIENT VISITS
fact_patient_visits = df_agg.select(
    "patient_id", "visit_id", "doctor_id", "hospital_id", "diagnosis", "visit_date", "treatment_cost"
)

# ✅ FACT: LAB RESULTS
fact_lab_results = df_agg.select(
    "patient_id", "visit_id", "lab_result_id", "test_type", "result_value", "test_date"
)

# COMMAND ----------

gold_path = "wasbs://healthcare127@healthcare127.blob.core.windows.net/gold_delta/full_healthcare_data"

# Save dimension tables
dim_patient.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_patient")
dim_hospital.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_hospital")
dim_doctor.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_doctor")
dim_date.write.format("delta").mode("overwrite").save(f"{gold_path}/dim_date")

# Save fact tables
fact_patient_visits.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_patient_visits")
fact_lab_results.write.format("delta").mode("overwrite").save(f"{gold_path}/fact_lab_results")

# COMMAND ----------

#Hospital performance
# # Load Delta tables
fact_visits = spark.read.format("delta").load(f"{gold_path}/fact_patient_visits")
dim_hospital = spark.read.format("delta").load(f"{gold_path}/dim_hospital")

#Step 2: Create Temp Views (for SQL analysis)

fact_visits.createOrReplaceTempView("fact_visits")
dim_hospital.createOrReplaceTempView("dim_hospital")




# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC     dim_hospital.region,
# MAGIC     COUNT(DISTINCT fact_visits.patient_id) AS unique_patients,
# MAGIC     COUNT(fact_visits.visit_id) AS total_visits,
# MAGIC     ROUND(AVG(fact_visits.treatment_cost), 2) AS avg_treatment_cost,
# MAGIC     SUM(DISTINCT dim_hospital.bed_capacity) AS total_beds,
# MAGIC     ROUND(COUNT(fact_visits.visit_id) * 1.0 / SUM(DISTINCT dim_hospital.bed_capacity), 2) AS utilization_ratio
# MAGIC FROM fact_visits
# MAGIC JOIN dim_hospital ON fact_visits.hospital_id = dim_hospital.hospital_id
# MAGIC GROUP BY dim_hospital.region
# MAGIC ORDER BY utilization_ratio DESC
# MAGIC

# COMMAND ----------

#Step 4: Most Common Diagnosis per Region

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, count

# Join visits with hospitals
diagnosis_by_region = fact_visits.join(dim_hospital, "hospital_id") \
    .groupBy("region", "diagnosis") \
    .agg(count("*").alias("cases"))

# Use window function to get top diagnosis per region
window_spec = Window.partitionBy("region").orderBy(diagnosis_by_region["cases"].desc())

top_diagnosis_per_region = diagnosis_by_region.withColumn("rank", row_number().over(window_spec)) \
    .filter("rank = 1") \
    .drop("rank")

top_diagnosis_per_region.show()

# COMMAND ----------

visits = spark.read.format("delta").load(f"{gold_path}/fact_patient_visits")
labs = spark.read.format("delta").load(f"{gold_path}/fact_lab_results")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, datediff, count

# Define window partitioned by patient and ordered by date
visit_window = Window.partitionBy("patient_id").orderBy("visit_date")

# Calculate days between visits
visits_with_diff = visits.withColumn("prev_visit", lag("visit_date").over(visit_window))
visits_with_diff = visits_with_diff.withColumn("days_between", datediff("visit_date", "prev_visit"))

# Count visits in short intervals (30 days)
visit_counts = visits.groupBy("patient_id") \
    .agg(count("visit_id").alias("visit_count"))

frequent_visitors = visit_counts.filter("visit_count >= 3")

display(frequent_visitors)

# COMMAND ----------

#Step 3: Patients with High Avg Treatment Cost

high_cost_patients = visits.groupBy("patient_id") \
    .agg(F.avg("treatment_cost").alias("avg_cost")) \
    .filter("avg_cost > 3000")

display(high_cost_patients)

# COMMAND ----------

#Step 4: Patients with Abnormal Lab Results (optional logic)
#You may adjust thresholds per test type based on domain input.

# Example: Abnormally high lab values (generic threshold > 8)
abnormal_results = labs.filter("result_value > 8") \
    .select("patient_id").distinct()

display(abnormal_results)

# COMMAND ----------

# Join all risk criteria
high_risk_patients = frequent_visitors \
    .join(high_cost_patients, "patient_id", "outer") \
    .join(abnormal_results, "patient_id", "outer") \
    .withColumn("high_risk_flag", F.lit(True))

high_risk_patients.select("patient_id", "high_risk_flag").distinct().show()
