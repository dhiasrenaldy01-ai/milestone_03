from pyspark.sql import SparkSession

def extract_data(input_path: str, output_path: str):
    """
    Extract data from CSV file using PySpark and save the result.

    Parameters:
    input_path (str): Path to raw CSV file
    output_path (str): Path to output folder (CSV format)
    """
    print("🚀 Extract process started")
    print("📂 Input file :", input_path)

    spark = SparkSession.builder \
        .appName("P2M3_Extract_Patient_Data") \
        .getOrCreate()

    # Extract (read CSV)
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )

    row_count = df.count()
    print("📊 Total rows extracted:", row_count)

    if row_count == 0:
        raise ValueError("❌ Extracted DataFrame is empty")

    # Save extract result (PySpark writes folder)
    df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)

    print("💾 Data saved to folder:", output_path)
    print("✅ Extract process completed")

    spark.stop()


# ===== Main Process =====
if __name__ == "__main__":
    input_file = "/opt/airflow/data/P2M3_dhias_renaldy_data_raw.csv"
    output_folder = "/opt/airflow/data/extract_result_patient_pipeline"

    extract_data(input_file, output_folder)
