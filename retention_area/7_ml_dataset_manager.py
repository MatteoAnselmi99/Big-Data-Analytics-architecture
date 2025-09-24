import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import first
import traceback, shutil, os, logging, sys
from BDA_Scripts.print_to_log import StreamToLogger

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

def update_ml_dataset(spark, current, historical):
    # READ THE PARQUET FILES
    c_df = spark.read.parquet(current)
    try :
        h_df = spark.read.parquet(historical)

        # DF UNION (CONTRACTS DUPLICATED)
        df_union = c_df.unionByName(h_df)

        # AGGREGATION AND SUM OF COLUMNS BY CONTRACTS
        columns_to_sum = [
            "total_meters", "meters_motorway", "meters_urban", "meters_other",
            "total_seconds", "seconds_motorway", "seconds_urban", "seconds_other",
            "meters_travelled_monday", "meters_travelled_tuesday", "meters_travelled_wednesday",
            "meters_travelled_thursday", "meters_travelled_friday", "meters_travelled_saturday",
            "meters_travelled_sunday", "seconds_travelled_monday", "seconds_travelled_tuesday",
            "seconds_travelled_wednesday", "seconds_travelled_thursday", "seconds_travelled_friday",
            "seconds_travelled_saturday", "seconds_travelled_sunday", "meters_travelled_day",
            "meters_travelled_night", "seconds_travelled_day", "seconds_travelled_night",
            "speeding", "acceleration", "brake", "cornering", "lateral_movement",
            "duration", "number_trips", "risk_6", "risk_5", "risk_4", "risk_3",
            "risk_2", "risk_1", "body_injuries", "property_damage", "body_and_property_damage",
            "atti_vandalici", "cardC", "cardD", "casco", "collision", "glass",
            "natural_events", "maindriver_injuries", "fire_theft", "rca", "incurred"]
        agg_exprs = ([_sum(column).alias(column) for column in columns_to_sum] +
                     [first("ingestion_date", ignorenulls=True).alias("ingestion_date")])
        df_aggregated = df_union.groupBy("contract").agg(*agg_exprs)

        # OVERWRITING GOLD_HISTORICAL
        df_aggregated.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .parquet(historical_gold_path)
        print("Gold dataset updated successfully")

    except Exception as e:
        print("No historical folder found. Moving current gold in historical gold folder.")
        traceback.print_exc()

        # FALLBACK
        c_df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .parquet(historical_gold_path)
        print("Fallback writing completed successfully.")

    finally:
        # DELETING DATA IN GOLD CURRENT FOLDER
        list_cmd = ['/opt/hadoop/bin/hdfs', 'dfs', '-ls', current]
        result = subprocess.run(list_cmd, capture_output=True, text=True, check=True)

        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 8:
                path = parts[-1]
                rm_cmd = ['/opt/hadoop/bin/hdfs', 'dfs', '-rm', '-r', path]
                subprocess.run(rm_cmd, check=True)
        print(f"Current Gold folder clean successfully.")

# ======================================================== APPLICATION
if __name__ == "__main__":
    # ========================== LOGS
    app_name = "update_ml_dataset"
    log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/7_ml_dataset_manager"
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

 # SILVER PATH
    current_gold_path = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/gold"
    historical_gold_path = "hdfs://localhost:9000/user/dr.who/ingestion_area/historical/gold"

    update_ml_dataset(spark, current_gold_path,historical_gold_path)

    spark.stop()