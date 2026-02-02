# 🏥 Automation Data Pipeline for Healthcare Patient Flow Analytics

## 📌 Project Overview
This project implements an automated data pipeline for healthcare patient flow analytics.  
The pipeline is designed to validate, transform, and load patient admission data into a NoSQL database, ensuring data quality and reliability before downstream analytics.

The workflow follows an end-to-end ETL process and is fully orchestrated using Apache Airflow, with a strong emphasis on data validation using Great Expectations.

---

## 🎯 Project Objectives
The main objectives of this project are to:
- Build an automated ETL data pipeline
- Perform data quality validation prior to automation
- Prepare raw healthcare data for analytical use
- Apply scalable data processing techniques
- Orchestrate workflows using Apache Airflow
- Store clean and validated data in a NoSQL database (MongoDB)

---

## 🧩 Dataset Description
- **Domain:** Healthcare – Patient Flow Analytics  
- **Source:** Public dataset from Kaggle  
- **Data Type:** CSV  

### Dataset Characteristics
- More than 10 columns
- Balanced categorical and numerical features
- Mixed-case column naming (capital and camel case)

### Key Columns
- Patient Name  
- Patient Age  
- Patient Gender  
- Patient Admission Date  
- Patient Admission Time  
- Patient Waittime  
- Patient Race  

---

## 🏗️ Pipeline Architecture
The pipeline is designed with a clear separation of concerns:

Raw CSV Data
      ↓
Pre-Automation Validation (Great Expectations)
      ↓
Extract
      ↓
Transform
      ↓
Load (MongoDB)
      ↓
Analytics-Ready Data

## 🧪 Pre-Automation: Data Validation
Before entering the automation pipeline, data is validated using Great Expectations to ensure data quality and consistency.

### Validation Rules Include
- Patient Admission Date must not be null  
- Patient Waittime must be greater than or equal to 0  
- Patient Age must be within a reasonable range  
- Patient Gender must contain valid values  
- Datetime conversion must be successful  

Only validated data proceeds to the automation stage.

---

## ⚙️ Automation Pipeline

### 1. Extract
- Reads raw CSV data from the Airflow data directory  
- Handles schema inference  
- Logs extraction results  

### 2. Transform
- Renames columns for consistency  
- Cleans invalid categorical values  
- Combines admission date and time into a single datetime column  
- Adds transformation timestamp for data lineage  

### 3. Load
- Loads transformed data into MongoDB  
- Uses document-based storage for flexibility  
- Prepares data for downstream analytics  

---

## ⏰ Workflow Orchestration
The pipeline is orchestrated using Apache Airflow with a DAG that manages task dependencies.

### DAG Flow

extract_task → transform_task → load_task


## Scheduling:

Runs every Saturday

Time window: 09:10 – 09:30 AM

Execution interval: every 10 minutes

## 🛠️ Tools & Technologies

- Python – Core programming language

- Pandas – Pre-automation data handling

- PySpark – Scalable data processing

- Great Expectations – Data validation

- Apache Airflow – Workflow orchestration

- MongoDB – NoSQL data storage

## 📈 Business Value

This automated pipeline enables:

- Improved healthcare data reliability

- Reduced data quality issues

- Faster availability of analytics-ready data

- Scalable patient flow analysis

- Better support for data-driven decision making

## 🚀 Future Improvements

- Potential enhancements for this project include:

- Alerting and notifications on validation failures

- Incremental data loading

- Data versioning

- Integration with BI tools for visualization

- Enhanced monitoring and logging

## 👤 Author

Dhias Renaldy Hendrawan
Data Engineer / Data Analyst
