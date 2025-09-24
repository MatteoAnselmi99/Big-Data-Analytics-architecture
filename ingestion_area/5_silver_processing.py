from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, concat_ws, to_timestamp, unix_timestamp, when
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import current_date
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

# =============================================== PRE- PROCESSING CLAIMS
def process_claims_and_save(spark, folder_path: str, silver_output_path: str):
    # READ ALL THE .PARQUET FILES IN THE CURRENT BRONZE FOLDER
    df = spark.read.parquet(folder_path)

    # SELECT ONLY PRODUCT = A AND (CLAIMS_FAULT >=50% OR CLAIM_TYPE = ARD)
    df = df.filter((col("product") == "A") &
                  ((col("pct_responsabilita") >= 50) | (col("clmtype") == "ARD")))

    # SELECT ONLY VALUABLE COLUMNS
    df = df.select("claimnumber", "policy", "clmclass", "clmtype2", "incurred")

    # FORMATTING 'INCURRED' COLUMN
    df = df.withColumn("incurred",
        regexp_replace(
            regexp_replace(
                col("incurred"), "\.", ""), ",", ".").cast("float"))

    # MANUAL ONE-HOT ENCODING
    clmclass_values = ["BODY INJURIES", "PROPERTY DAMAGE", "PROPERTY DAMAGE - BODY INJURIES"]
    clmtype2_values = ["AV", "CARDC", "CARDD", "CAS", "COLL", "CRI", "EN", "IC", "IF", "RCAF"]

    for value in clmclass_values:
        df = df.withColumn(f"clmclass_{value}", (col("clmclass") == value).cast("int"))

    for value in clmtype2_values:
        df = df.withColumn(f"clmtype2_{value}", (col("clmtype2") == value).cast("int"))

    # RENAME COLUMNS
    df = df.withColumnRenamed("policy", "contract") \
        .withColumnRenamed("clmclass_BODY INJURIES", "body_injuries") \
        .withColumnRenamed("clmclass_PROPERTY DAMAGE", "property_damage") \
        .withColumnRenamed("clmclass_PROPERTY DAMAGE - BODY INJURIES", "body_and_property_damage") \
        .withColumnRenamed("clmtype2_AV", "atti_vandalici") \
        .withColumnRenamed("clmtype2_CARDC", "cardC") \
        .withColumnRenamed("clmtype2_CARDD", "cardD") \
        .withColumnRenamed("clmtype2_CAS", "casco") \
        .withColumnRenamed("clmtype2_COLL", "collision") \
        .withColumnRenamed("clmtype2_CRI", "glass") \
        .withColumnRenamed("clmtype2_EN", "natural_events") \
        .withColumnRenamed("clmtype2_IC", "maindriver_injuries") \
        .withColumnRenamed("clmtype2_IF", "fire_theft") \
        .withColumnRenamed("clmtype2_RCAF", "rca")

    # GROUP BY CONTRACT AND SUM THE CLAIMS OCCURRED
    columns_to_sum = [
        "body_injuries", "property_damage", "body_and_property_damage", "atti_vandalici", "cardC", "cardD", "casco",
        "collision", "glass", "natural_events", "maindriver_injuries", "fire_theft", "rca", "incurred"]

    df_final = df.groupBy("contract").sum(*columns_to_sum)

    # RENAMES THE "SUM" COLUMNS CREATED
    for col_name in columns_to_sum:
        df_final = df_final.withColumnRenamed(f"sum({col_name})", col_name)

    # WRITE DATA .PARQUET IN SILVER FOLDER
    df_final = df_final.withColumn("ingestion_date", current_date())
    df_final.write.mode("append").partitionBy("ingestion_date").parquet(silver_output_path)
    print(f"Saved in Silver: {silver_output_path}")
    del df_final

# ========================================== COLUMN VALUE VALIDATION
def clean_column_names(df):
    for old in df.columns:
        new = old.lower().strip().replace(" ", "_").replace(";", "") \
            .replace(",", "").replace(".", "").replace("-", "_") \
            .replace("(", "").replace(")", "")
        df = df.withColumnRenamed(old, new)
    return df
#======================================================= TLM FUNCTION
def join_bronze_tables(spark, behaviour, province, trip_summary, silver_output_path):
    # READ ALL THE TLM FILE IN BRONZE FOLDER
    behaviour_df = spark.read.parquet(behaviour)
    province_df = spark.read.parquet(province)
    trip_summary_df = spark.read.parquet(trip_summary)

    # CLEAN COLUMN NAMES
    behaviour_df = clean_column_names(behaviour_df)
    province_df = clean_column_names(province_df)
    trip_summary_df = clean_column_names(trip_summary_df)

    # --------------------------------------------- PRE-PROCESSING BEHAVIOUR
    # DROP NOT VALUABLE COLUMNS
    cols_to_drop = ['type', 'tins', 'number_of_events_2_digit', 'flusso_timestamp', 'number_of_events',
                    'ingestion_timestamp','source_file']
    behaviour_df = behaviour_df.drop(*cols_to_drop)

    # DROP ROWS HAVING NULL VALUES IN ESSENTIAL COLUMNS
    behaviour_df = behaviour_df.dropna(subset=['contract_number', 'voucher_number', 'event_code', 'trip_id'])

    # ONE-HOT ENCODING
    def one_hot_encode(df, column, values=None):
        if not values:
            values = [row[column] for row in df.select(column).distinct().collect()]
        for v in values:
            df = df.withColumn(f"{column}_{v}", when(col(column) == v, 1).otherwise(0))
        return df

    behaviour_df = one_hot_encode(behaviour_df, "event_code", ['11', '21', '23', '25', '27'])
    behaviour_df = one_hot_encode(behaviour_df, "road_type", ['M', 'U', 'O'])
    behaviour_df = one_hot_encode(behaviour_df, "day_time", ['D', 'N'])
    behaviour_df = one_hot_encode(behaviour_df, "week_day", ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN'])

    # RENAME COLUMNS
    rename_dict = {
        'event_code_11': 'speeding',
        'event_code_21': 'acceleration',
        'event_code_23': 'brake',
        'event_code_25': 'cornering',
        'event_code_27': 'lateral_movement',
        'road_type_M': 'road_motorways',
        'road_type_U': 'road_urban',
        'road_type_O': 'road_other',
        'day_time_D': 'road_day',
        'day_time_N': 'road_night',
        'week_day_MON': 'monday',
        'week_day_TUE': 'tuesday',
        'week_day_WED': 'wednesday',
        'week_day_THU': 'thursday',
        'week_day_FRI': 'friday',
        'week_day_SAT': 'saturday',
        'week_day_SUN': 'sunday',}

    for old_col, new_col in rename_dict.items():
        behaviour_df = behaviour_df.withColumnRenamed(old_col, new_col)

    # CAST FROM STRING TO INT
    array_int = ['speeding', 'acceleration', 'brake', 'cornering', 'lateral_movement',
                 'road_motorways', 'road_urban', 'road_other',
                 'road_day', 'road_night',
                 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    for colname in array_int:
        behaviour_df = behaviour_df.withColumn(colname, col(colname).cast("int"))

    # GROUP BY TRIP_ID AND SUM THE EVENTS
    behaviour_df = behaviour_df.groupBy("trip_id").agg(
        _sum("speeding").alias("speeding"),
        _sum("acceleration").alias("acceleration"),
        _sum("brake").alias("brake"),
        _sum("cornering").alias("cornering"),
        _sum("lateral_movement").alias("lateral_movement"))
    #------------------------------------------------ PRE-PROCESSING PROVINCE
    # DROP NOT VALUABLE COLUMNS
    cols_to_drop = ['type', 'flusso_timestamp', 'meters_travelled', 'tins', 'ingestion_timestamp',
                    'source_file','voucher_number','ingestion_date']
    province_df = province_df.drop(*cols_to_drop)

    # DROP ROWS HAVING NULL VALUES IN ESSENTIAL COLUMNS
    province_df = province_df.dropna(subset=['contract_number', 'province_code', 'trip_id'])

    # REMOVE DUPLICATES
    province_df = province_df.dropDuplicates()

    # ---------------------------------------- PRE-PROCESSING SUMMARY
    # DROP NOT VALUABLE COLUMNS
    cols_to_drop = ['type', 'tins', 'flusso_timestamp', 'contract_number', 'meters_travelled_5', 'time_travelled_5',
                    'ingestion_timestamp', 'source_file','voucher_number','ingestion_date']
    trip_summary_df = trip_summary_df.drop(*cols_to_drop)

    # RENAME COLUMNS
    rename_map = {
        'meters_travelled_1': 'total_meters',
        'meters_travelled_2': 'meters_motorway',
        'meters_travelled_3': 'meters_urban',
        'meters_travelled_4': 'meters_other',
        'time_travelled_1': 'total_seconds',
        'time_travelled_2': 'seconds_motorway',
        'time_travelled_3': 'seconds_urban',
        'time_travelled_4': 'seconds_other',
    }
    for old_col, new_col in rename_map.items():
        trip_summary_df = trip_summary_df.withColumnRenamed(old_col, new_col)

    # CAST CONTRACT COLUMN FROM INT TO STRING
    trip_summary_df = trip_summary_df.withColumn("contract", col("contract").cast("string"))

    # DROP DUPLICATES
    trip_summary_df = trip_summary_df.dropDuplicates()

    # --------------------------------- JOIN
    # JOIN: behaviour + province + trip_summary
    final_df = trip_summary_df \
        .join(behaviour_df, on="trip_id", how="outer") \
        .join(province_df, on="trip_id", how="outer")
    print(f"Join completed. final_df rows: {final_df.count()}")

    # --------------------------------- PROCESSING JOIN
    # MERGING START DATE & START TIME, END DATE & END TIME
    final_df = final_df.filter(
        (col("start_date") != "") & (col("start_time") != "") & (col("end_date") != "") & (col("end_time") != ""))

    final_df = final_df.withColumn("start_datetime",to_timestamp(concat_ws(" ", col("start_date"),
                                                                           col("start_time")), "yyyy-MM-dd HH:mm:ss"))

    final_df = final_df.withColumn("end_datetime",to_timestamp(concat_ws(" ", col("end_date"),
                                                                         col("end_time")),"yyyy-MM-dd HH:mm:ss"))

    # GAP BETWEEN END-DATETIME AND START-DATETIME = DURATION
    final_df = final_df.withColumn("duration",unix_timestamp("end_datetime") - unix_timestamp("start_datetime"))

    # REMOVES NOT VALUABLE COLUMNS
    cols_to_drop = ['voucher_number_x', 'contract_number', 'voucher_number_y', 'start_date', 'end_date',
                    'start_time','end_time']
    final_df = final_df.drop(*cols_to_drop)

    # FILLING NULL VALUES
    final_df = final_df.fillna(0)

    # CASTING FROM FLOAT TO INT
    float_cols = [f.name for f in final_df.schema.fields if f.dataType.simpleString() == 'double']
    for colname in float_cols:
        final_df = final_df.withColumn(colname, col(colname).cast(IntegerType()))

    # WRITE .PARQUET FILE IN SILVER FOLDER, PARTITIONED BY DAY
    final_df = final_df.withColumn("ingestion_date", current_date())
    final_df.write.mode("append").partitionBy("ingestion_date").parquet(silver_output_path)
    print(f"Saved in Silver: {silver_output_path}")

#============================================================================================= Applicazione

if __name__ == "__main__":
    # ========================== LOGS
    app_name = "silver_processing"
    log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/5_silver_processing"
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

    # BRONZE PATHS
    behaviour = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/telematics/behaviour"
    province = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/telematics/province"
    trip_summary = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/telematics/trip_summary"
    claims = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/claims"

    # SILVER PATHS
    silver_path_tlm = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/silver/telematics"
    silver_path_clm = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/silver/claims"

    join_bronze_tables(spark, behaviour, province, trip_summary, silver_path_tlm)
    process_claims_and_save(spark,claims,silver_path_clm)

    spark.stop()