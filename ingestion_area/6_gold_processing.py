from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import col, when
import os, logging, sys
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

def join_silver_tables(spark, tlm, claims, gold_output_path):
    # READ THE PARQUET FILES
    tlm_df = spark.read.parquet(tlm)
    clm_df = spark.read.parquet(claims)

    tlm_df = tlm_df.withColumn("province_code", col("province_code").cast("string"))
    # PROVINCE AGGREGATION FOR RISK-LEVEL (Population/n_claims)
    risk_6 = ['RM', 'MI']
    risk_5 = ['NA', 'TO', 'FI', 'GE', 'BO', 'BA', 'CT']
    risk_4 = ['VR', 'BS', 'BG', 'PA', 'MO', 'PD', 'SA', 'MB', 'VE', 'VA', 'VI']
    risk_3 = ['TV', 'LE', 'BZ', 'RE', 'LU', 'LT', 'PG', 'RA', 'LI', 'RN', 'AN', 'PI', 'FC', 'CE', 'TA', 'PR', 'SS',
              'CO', 'ME', 'PV', 'TN', 'SV', 'FG', 'UD', 'PU','AL', 'FE', 'CN', 'PC', 'FR']
    risk_2 = ["CR", "IM", "BR", "MC", "MN", "NO", "PT", "PO", "RC", "CS", "CA", "TS", "PE", "SR", "AR", "TP", "GR",
              "BT", "MS", "VT", "LC","SI", "CH", "TE", "AP", "SP", "PN", "RG", "TR", "AQ", "CL", "AG", "RO", "CZ", "PZ",
              "FM", "AV", "SU","LO", "BL", "MT", "SO","GO"]
    risk_1 = ['VC', 'AT', 'BI', 'RI', 'VB', 'NU', 'CB', 'BN', 'KR', 'OR', 'VV', 'EN', 'IS','AO']
    tlm_df = tlm_df \
        .withColumn("risk_6", when(col("province_code").isin(risk_6), 1).otherwise(0)) \
        .withColumn("risk_5", when(col("province_code").isin(risk_5), 1).otherwise(0)) \
        .withColumn("risk_4", when(col("province_code").isin(risk_4), 1).otherwise(0)) \
        .withColumn("risk_3", when(col("province_code").isin(risk_3), 1).otherwise(0)) \
        .withColumn("risk_2", when(col("province_code").isin(risk_2), 1).otherwise(0)) \
        .withColumn("risk_1", when(col("province_code").isin(risk_1), 1).otherwise(0))

    # AGGREGATING TRIPS BY CONTRACT
    agg_cols = ['total_meters', 'meters_motorway', 'meters_urban', 'meters_other',
                'total_seconds', 'seconds_motorway', 'seconds_urban', 'seconds_other',
                'meters_travelled_monday', 'meters_travelled_tuesday', 'meters_travelled_wednesday',
                'meters_travelled_thursday', 'meters_travelled_friday', 'meters_travelled_saturday',
                'meters_travelled_sunday', 'seconds_travelled_monday', 'seconds_travelled_tuesday',
                'seconds_travelled_wednesday', 'seconds_travelled_thursday', 'seconds_travelled_friday',
                'seconds_travelled_saturday', 'seconds_travelled_sunday', 'meters_travelled_day',
                'meters_travelled_night', 'seconds_travelled_day', 'seconds_travelled_night',
                'speeding', 'acceleration', 'brake', 'cornering', 'lateral_movement', 'duration',
                'number_trips','risk_6', 'risk_5', 'risk_4', 'risk_3', 'risk_2', 'risk_1']
    tlm_df = tlm_df.groupBy("contract").agg(*[_sum(col(c)).alias(c) for c in agg_cols])

    # JOIN CLAIMS AND TELEMATICS BY CONTRACT
    df = tlm_df.join(clm_df, on="contract", how="left")

    # SAVE PARQUET FILE IN GOLD_CURRENT, PARTITIONED BY DAY
    df.write.mode("append").partitionBy("ingestion_date").parquet(gold_output_path)
    print(f"Appended in Gold: {gold_output_path}")

# ======================================================== APPLICATION
if __name__ == "__main__":
    # ========================== LOGS
    app_name = "gold_processing"
    log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/6_gold_processing"
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
    silver_path_tlm = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/silver/telematics"
    silver_path_clm = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/silver/claims"

# GOLD PATH
    gold_path = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/gold"

    join_silver_tables(spark, silver_path_tlm,silver_path_clm,gold_path)
    spark.stop()
#----------------------------------