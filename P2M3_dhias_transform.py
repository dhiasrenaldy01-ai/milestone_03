from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    upper,
    trim,
    to_timestamp,
    current_timestamp
)


def transform_data(input_path: str, output_path: str):
    """
    Transform patient data:
    1. Rename column 'Merged' to 'Patient Name'
    2. Combine admission date and time into datetime
    3. Add transform timestamp
    4. Save transformed data

    Parameters:
    input_path (str): Path to extracted CSV file
    output_path (str): Path to save transformed CSV file
    """
    print("🚀 Transform process started")
    print("📂 Reading data from:", input_path)

    spark = SparkSession.builder.getOrCreate()

    # =========================
    # Load extracted data
    # =========================
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    if df.rdd.isEmpty():
        raise ValueError("❌ Input DataFrame is empty")

    # =========================
    # 1. Rename column
    # =========================
    if "Merged" in df.columns:
        df = df.withColumnRenamed("Merged", "Patient Name")
        print("✅ Column 'Merged' renamed to 'Patient Name'")

    # =========================
    # 2. Combine Date & Time → Datetime
    # =========================
    df = df.withColumn(
        "Admission_DateTime",
        to_timestamp(
            concat_ws(
                " ",
                trim(col("Patient Admission Date")),
                upper(trim(col("Patient Admission Time")))
            ),
            "d/M/yyyy h:mm:ss a"
        )
    )

    # =========================
    # 3. Add transformation timestamp
    # =========================
    df = df.withColumn("transform_datetime", current_timestamp())

    print("📊 Total rows after transform:", df.count())
    df.printSchema()

    # =========================
    # 4. Save transformed data
    # =========================
    (
        df
        .coalesce(1)  # biar 1 file CSV (opsional, tapi umum di task akademik)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    print("💾 Transformed data saved to:", output_path)
    print("✅ Transform process completed")


# ===== Main Process =====
if __name__ == "__main__":
    input_file = "/opt/airflow/data/extract_result_patient_pipeline.csv"
    output_file = "/opt/airflow/data/transform_result_patient_pipeline"

    transform_data(input_file, output_file)
