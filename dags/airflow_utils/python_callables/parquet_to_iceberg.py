from pyspark.sql import SparkSession
from airflow_utils.utils.constants import PARQUET_ROOT_FOLDER, ICEBERG_DATABASE_STAGING_1, ICEBERG_STANDARD_CATALOG

# Define the processing function
def process_parquet_to_iceberg(path, filename, table_name):
    catalog_name = ICEBERG_STANDARD_CATALOG
    db_name = ICEBERG_DATABASE_STAGING_1
    
    parquet_file_path = f"{PARQUET_ROOT_FOLDER}/{path}/{filename}.parquet"

    # Initialize Spark session with Iceberg configurations
    spark = SparkSession.builder \
        .appName("Parquet to Iceberg") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3://warehouse/") \
        .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    # Read the Parquet file into a DataFrame
    df = spark.read.parquet(parquet_file_path)
    
    df.head()

    # # Define the Iceberg table name
    # iceberg_table_name = f"{catalog_name}.{db_name}.{table_name}"  # Replace with your catalog and table name

    # # Write the DataFrame to the Iceberg table
    # df.write.format("iceberg").mode("append").save(iceberg_table_name)

    # # Stop the Spark session
    # spark.stop()