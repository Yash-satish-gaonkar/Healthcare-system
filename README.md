# Healthcare-system

# Project Overview
This end-to-end healthcare analytics project demonstrates the use of modern data engineering practices to ingest, transform, model, and analyze healthcare data in Azure. The dataset includes patient visits, hospital details, lab results, and prescriptions.The solution enables healthcare administrators to monitor patient loads, analyze diagnoses, evaluate hospital performance, and proactively flag high-risk patients based on historical data.

# Project Requirements
1. Data Ingestion
Load raw CSV files from Azure Blob Storage into Azure Databricks
Infer schema and store data in Delta format in the Bronze layer
Enable SQL-based querying using Spark SQL

2. Data Transformation
Clean and normalize data (handle nulls, enrich timestamps, unify formats)
Create structured dimension and fact tables in the Silver layer
Store curated tables in Delta format with partitioning for efficiency

3. Data Modeling
Model the data using a star schema
Dimension Tables: Patient, Hospital, Doctor, Date
Fact Tables: Patient Visits, Lab Results, Prescriptions

4. Advanced Analysis
Identify high-risk patients based on:
- Frequent hospital visits
- High treatment costs
- Abnormal lab results
- Analyze regional diagnosis trends and hospital performance
- Calculate utilization ratios (visits vs bed capacity) per region

5. Business Intelligence
Visualize KPIs such as:
- Top diagnoses per region
- High-risk patient distribution
- Hospital performance metrics
- Doctor-patient workload trends

# Solution Architecture
This project follows the Multi-Hop Architecture approach with three refined data layers:

1. Bronze Layer (Raw Ingestion)
Direct copy of CSVs from Blob
Schema inferred, data written as Delta tables

2. Silver Layer (Cleansed & Structured)
Nulls handled, schema normalized
Created star schema: fact and dimension tables

3. Gold Layer (Final Analytics)
Includes aggregates, joins, and flags (e.g., high_risk_flag)

# Analysis with Spark SQL
The project performs several key analyses using spark.sql:
- Hospital Performance by Region: Utilization ratios, average costs, and visit volumes
- High-Risk Patient Identification: Based on cost, frequency, and lab abnormalities
- Diagnosis Trends: Most common conditions by region
- Doctor Load Analysis: Number of patients per doctor
- Regional Healthcare Insights: Bed capacity vs. patient load

# Project Outcome
1. Created a production-grade pipeline for ingesting and transforming healthcare data
2. Defined dimensional and fact models for analytics
3. Developed insights to help hospitals:
    - Predict and manage load
    - Identify critical patients early
    - Track performance at the hospital and regional level
