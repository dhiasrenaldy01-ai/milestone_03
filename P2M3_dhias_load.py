from pyspark.sql import SparkSession
from pymongo import MongoClient

def load_data(input_path: str):
    """
    Load transformed patient data from PySpark DataFrame
    into MongoDB.

    Parameters:
    input_path (str): Path to transformed CSV folder
    """
    print("🚀 Load process started")
    print("📂 Reading transformed data from:", input_path)

    spark = SparkSession.builder \
        .appName("P2M3_Load_Patient_Data") \
        .getOrCreate()

    # Read transformed data (CSV folder)
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )

    row_count = df.count()
    if row_count == 0:
        raise ValueError("❌ No data to load")

    print("📊 Total rows to load:", row_count)

    # Convert Spark DataFrame → list of dict
    document_list = [row.asDict() for row in df.collect()]

    # Connect to MongoDB
    client = MongoClient(
        "mongodb+srv://post:post@dhias-renaldy.cl63raf.mongodb.net/"
    )

    db = client["patient"]
    collection = db["data_patient"]

    # Insert data
    collection.insert_many(document_list)

    print("💾 Data successfully loaded to MongoDB")
    print("✅ Load process completed")

    spark.stop()


# ===== Main Process =====
if __name__ == "__main__":
    input_folder = "/opt/airflow/data/transform_result_patient_pipeline"

    load_data(input_folder)
