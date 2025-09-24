from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
import os, logging, sys, subprocess
from pyspark.sql.functions import current_date
from BDA_Scripts.print_to_log import StreamToLogger

# LOGS
def set_spark_python_logs(app_name, log_base_dir):
    log_dir = os.path.join(log_base_dir, app_name)
    os.makedirs(log_dir, exist_ok=True)

    # SPARK LOGS
    log4j_path = os.path.join(log_dir, "log4j.properties")
    with open(log4j_path, "w") as f:
        f.write(f"""
                log4j.rootCategory=INFO, FILE
                log4j.appender.FILE=org.apache.log4j.RollingFileAppender
                log4j.appender.FILE.File={log_dir}/spark.log
                log4j.appender.FILE.MaxFileSize=10MB
                log4j.appender.FILE.MaxBackupIndex=5
                log4j.appender.FILE.layout=org.apache.log4j.PatternLayout
                log4j.appender.FILE.layout.ConversionPattern=%d{{yy/MM/dd HH:mm:ss}} %p %c{{1}}: %m%n
                """)

    # PYTHON LOGS
    log4p_path = os.path.join(log_dir, "python.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[logging.FileHandler(log4p_path)])
    logger = logging.getLogger(__name__)
    sys.stdout = StreamToLogger(logger, logging.INFO)
    sys.stderr = StreamToLogger(logger, logging.ERROR)
    return log4j_path

def ingest_csv_to_bronze(spark, staging_path, bronze_path):
    print(f"Ingestion from : {staging_path}")

    # IF IT'S CLAIMS CSV, USE ; AS DELIMITER
    if 'claims' in str(staging_path):
        df_raw = spark.read.option("header", True).option("sep", ";").csv(staging_path)
    else:
        df_raw = spark.read.option("header", True).csv(staging_path)

    if df_raw.rdd.isEmpty():
        print("No files found, ingestion skipped.")
        return

    # ADDING METADATA (TIMESTAMP AND NAME OF THE SOURCE FILE)
    df_enriched = df_raw.withColumn("ingestion_timestamp", current_timestamp()) \
                        .withColumn("source_file", input_file_name())

    # WRITE IN .PARQUET, PARTITIONING BY DAY
    df_enriched = df_enriched.withColumn("ingestion_date", current_date())
    df_enriched.write \
        .mode("append") \
        .partitionBy("ingestion_date") \
        .parquet(bronze_path)
    print(f"{df_enriched.count()} Rows wrote in : {bronze_path}")


# LIST ALL THE OBJECT IN THE FOLDER
def list_csv_files_hdfs(hdfs_folder):
    files = []
    cmd = ['/opt/hadoop/bin/hdfs', 'dfs', '-ls', hdfs_folder]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    for line in result.stdout.splitlines():
        parts = line.split()
        if parts and parts[-1].endswith('.csv'): # THE LAST ELEMENT OF THE OUTPUT IS THE PATH
            files.append(parts[-1])
    return files

def move_csv_files(source_dir_str, dest_dir_str):
    files_csv = list_csv_files_hdfs(source_dir_str)

    for file_path in files_csv:
        file_name = os.path.basename(file_path)
        dest = os.path.join(dest_dir_str, file_name)

        cmd = ["/opt/hadoop/bin/hdfs", "dfs", "-mv", file_path, dest]
        subprocess.run(cmd, check=True)
        print(f"Moved : {file_name} in {dest}")

if __name__ == "__main__":
    # ========================== LOGS
    app_name = "ingestion_file_mover"
    log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/4_ingestion_file_mover"
    log4j_path = set_spark_python_logs(app_name, log_base_dir)

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.mergeSchema", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.debug.maxToStringFields", 100) \
        .config("spark.sql.hive.convertMetastoreParquet", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_path}") \
        .config("spark.executor.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_path}") \
        .getOrCreate()

    ingest_csv_to_bronze(spark,
                         staging_path="hdfs://localhost:9000/user/dr.who/staging_area/approved/claims",
                         bronze_path="hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/claims")

    ingest_csv_to_bronze(spark,
                         staging_path="hdfs://localhost:9000/user/dr.who/staging_area/approved/telematics/behaviour",
                         bronze_path="hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/telematics/behaviour")

    ingest_csv_to_bronze(spark,
                         staging_path="hdfs://localhost:9000/user/dr.who/staging_area/approved/telematics/province",
                         bronze_path="hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/telematics/province")

    ingest_csv_to_bronze(spark,
                         staging_path="hdfs://localhost:9000/user/dr.who/staging_area/approved/telematics/trip_summary",
                         bronze_path="hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/telematics/trip_summary")
    spark.stop()

    move_csv_files('/user/dr.who/staging_area/approved/claims',
                   '/user/dr.who/staging_area/ingested/claims')

    move_csv_files('/user/dr.who/staging_area/approved/claims',
                   '/user/dr.who/staging_area/ingested/claims')
    
    move_csv_files('/user/dr.who/staging_area/approved/claims',
                   '/user/dr.who/staging_area/ingested/claims')
    
    move_csv_files('/user/dr.who/staging_area/approved/claims',
                   '/user/dr.who/staging_area/ingested/claims')