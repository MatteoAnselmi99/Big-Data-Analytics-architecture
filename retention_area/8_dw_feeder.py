from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.functions import regexp_replace, to_date, col
from pyspark.sql.types import IntegerType
import os,shutil, sys, logging, subprocess, datetime, boto3
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

def backup_bronze(local_dir, access_key, secret_key, bucket_name, s3_prefix):
    # TIMESTAMP FOR THE BACKUP
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    remote_prefix = f"{s3_prefix}/backup_{timestamp}/"

    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url='https://s3.eu-north-1.amazonaws.com'
    )

    # BACKUP OF THE WHOLE DIRECTORY
    local_tmp = Path("/home/matteo/BDA_project/ingestion_area/current/bronze/claims")
    local_tmp.mkdir(exist_ok=True)

    # LIST HDFS FILES
    cmd = ["/opt/hadoop/bin/hdfs", "dfs", "-ls", local_dir]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)

    for line in result.stdout.splitlines():
        if line.startswith('-'):
            parts = line.split()
            hdfs_file_path = parts[-1]
            local_file = local_tmp / Path(hdfs_file_path).name

            # DOWNLOAD EACH FILE IN CACHE
            subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-get", hdfs_file_path, str(local_file)], check=True)

            try:
                # UPLOAD .PARQUET IN S3 BUCKET
                s3.upload_file(str(local_file), bucket_name, remote_prefix)
                logging.info(f"Uploaded : {local_file} in s3://{bucket_name}/{remote_prefix}")

                # DELETE TEMPORARY FILE IN CACHE
                if os.path.isfile(local_file) or os.path.islink(local_file):
                    os.remove(local_file)
                elif os.path.isdir(local_file):
                    shutil.rmtree(local_file)
            except Exception as e:
                logging.error(f"Error uploading {local_file} : {e}")

def write_to_postgres(df, table, postgres_url, db_user, db_password):
    # POSTGRES INSERT QUERY
    try:
        df.write \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", postgres_url) \
            .option("dbtable", table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .mode("append") \
            .save()
        print(f"Rows added in table : {table}")

    except Exception as e:
        print(f"Error writing in the table : {table}: {e}")

def feed_behaviour_tables(spark, curr_path, postgres_url, db_user, db_password) :
    # LOAD PARQUET FILES
    df = spark.read.parquet(curr_path)
    df = df.withColumn("ingestion_date", regexp_replace(col("ingestion_date"), r" \+00$", ""))
    df= df.withColumn("ingestion_date", to_date(col("ingestion_date"), "yyyy-MM-dd"))

    # CREATE A SUBSET FOR EACH EVENT_TYPE
    df_11 = df.filter(df.event_code == "11")
    df_21 = df.filter(df.event_code == "21")
    df_23 = df.filter(df.event_code == "23")
    df_25 = df.filter(df.event_code == "25")
    df_27 = df.filter(df.event_code == "27")

    # SELECT ONLY THE COLUMNS IN POSTGRES TABLE
    df_11 = df_11.select("trip_id", "road_type", "day_time", "ingestion_date")
    df_21 = df_21.select("trip_id", "road_type", "day_time", "ingestion_date")
    df_23 = df_23.select("trip_id", "road_type", "day_time", "ingestion_date")
    df_25 = df_25.select("trip_id", "road_type", "day_time", "ingestion_date")
    df_27 = df_27.select("trip_id", "road_type", "day_time", "ingestion_date")

    # APPEND THE DATA IN THE DB TABLES
    write_to_postgres(df_11, "dim_speedings", postgres_url, db_user, db_password)
    write_to_postgres(df_21, "dim_accelerations", postgres_url, db_user, db_password)
    write_to_postgres(df_23, "dim_brakes", postgres_url, db_user, db_password)
    write_to_postgres(df_25, "dim_corners", postgres_url, db_user, db_password)
    write_to_postgres(df_27, "dim_lateral_movements", postgres_url, db_user, db_password)

def feed_summary_tables(spark, curr_path, postgres_url, db_user, db_password) :
    # LOAD PARQUET
    df = spark.read.parquet(curr_path)
    df = df.withColumn("ingestion_date", regexp_replace(col("ingestion_date"), r" \+00$", ""))
    df = df.withColumn("ingestion_date", to_date(col("ingestion_date"), "yyyy-MM-dd"))
    df = df.withColumnRenamed("meters_travelled_2", "meters_motorway")
    df = df.withColumnRenamed("meters_travelled_3", "meters_urban")
    df = df.withColumnRenamed("meters_travelled_4", "meters_other")
    df = df.withColumnRenamed("time_travelled_2", "seconds_motorway")
    df = df.withColumnRenamed("time_travelled_3", "seconds_urban")
    df = df.withColumnRenamed("time_travelled_4", "seconds_other")

    # CHANGE FORMAT FROM STRING TO INT
    columns_to_cast = ["seconds_motorway", "seconds_urban", "seconds_other",
        "seconds_travelled_monday", "seconds_travelled_tuesday", "seconds_travelled_wednesday",
        "seconds_travelled_thursday", "seconds_travelled_friday", "seconds_travelled_saturday",
        "seconds_travelled_sunday", "seconds_travelled_day", "seconds_travelled_night",
        "meters_motorway", "meters_urban", "meters_other",
        "meters_travelled_monday", "meters_travelled_tuesday", "meters_travelled_wednesday",
        "meters_travelled_thursday", "meters_travelled_friday", "meters_travelled_saturday",
        "meters_travelled_sunday", "meters_travelled_day", "meters_travelled_night"]
    for c in columns_to_cast:
        df = df.withColumn(c, col(c).cast(IntegerType()))

    # SELECT ONLY THE COLUMNS IN POSTGRES TABLES
    df_seconds = df.select("trip_id", "seconds_motorway", "seconds_urban", "seconds_other",
        "seconds_travelled_monday", "seconds_travelled_tuesday", "seconds_travelled_wednesday",
        "seconds_travelled_thursday", "seconds_travelled_friday", "seconds_travelled_saturday",
        "seconds_travelled_sunday", "seconds_travelled_day", "seconds_travelled_night", "ingestion_date")

    df_meters = df.select("trip_id","meters_motorway", "meters_urban", "meters_other",
        "meters_travelled_monday", "meters_travelled_tuesday", "meters_travelled_wednesday",
        "meters_travelled_thursday", "meters_travelled_friday", "meters_travelled_saturday",
        "meters_travelled_sunday", "meters_travelled_day", "meters_travelled_night", "ingestion_date")

    # APPEND THE DATA IN THE DB TABLES
    write_to_postgres(df_meters, "dim_meters_travelled", postgres_url, db_user, db_password)
    write_to_postgres(df_seconds, "dim_seconds_travelled", postgres_url, db_user, db_password)

def feed_silver_tables(spark, silver, postgres_url, db_user, db_password):
    # LOAD PARQUET
    df = spark.read.parquet(silver)
    df = df.withColumn("ingestion_date", regexp_replace(col("ingestion_date"), r" \+00$", ""))
    df = df.withColumn("ingestion_date", to_date(col("ingestion_date"), "yyyy-MM-dd"))
    df = df.withColumn("total_meters", col("total_meters").cast(IntegerType()))
    df = df.withColumn("total_seconds", col("total_seconds").cast(IntegerType()))

    # SELECT ONLY THE COLUMNS IN POSTGRES TABLES
    df = df.select("trip_id", "contract", "start_datetime", "end_datetime", "duration", "total_meters", "total_seconds",
                   "speeding","acceleration","brake","cornering","lateral_movement","ingestion_date","province_code")

    # APPEND THE DATA IN THE DB TABLE
    write_to_postgres(df, "fact_trip", postgres_url, db_user, db_password)


def feed_hive_tables():
    # EXECUTE .SH SCRIPT
    script_path = "./hive_update.sh"

    if not os.path.isfile(script_path):
        print(f" Script is missing in : {script_path}")
        return

    try:
        result = subprocess.run([script_path],check=True,capture_output=True,text=True)
        print("Script successfully executed")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error, script execution failed")
        print(e.stderr)

def feed_historical_with_current(spark, current, historical):
    # LOAD PARQUET
    df_current = spark.read.parquet(current)

    # APPEND DATA IN HISTORICAL FOLDER
    df_current.write.mode("append").partitionBy("ingestion_date").parquet(historical)
    print(f"{current} moved in : {historical}")

    # DELETING DATA IN CURRENT FOLDER
    list_cmd = ['/opt/hadoop/bin/hdfs', 'dfs', '-ls', current]
    result = subprocess.run(list_cmd, capture_output=True, text=True, check=True)

    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 8:
            path = parts[-1]
            rm_cmd = ['/opt/hadoop/bin/hdfs', 'dfs', '-rm', '-r', path]
            subprocess.run(rm_cmd, check=True)
    print(f"Current Gold folder clean successfully.")

#================================================================================= APPLICATION
if __name__ == "__main__":
    #========================== LOGS
    app_name = "dw_feeder"
    log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/8_dw_feeder"
    log4j_path = set_spark_python_logs(app_name, log_base_dir)
    #================================== SPARK SESSION
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/retention_area/postgresql-42.7.6.jar") \
        .config("spark.sql.parquet.mergeSchema", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.debug.maxToStringFields", 100) \
        .config("spark.sql.hive.convertMetastoreParquet", "true") \
        .config("spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_path}") \
        .config("spark.executor.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_path}") \
        .getOrCreate()

# CURRENT PATHS
    behaviour = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/telematics/behaviour"
    province = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/telematics/province"
    trip_summary = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/telematics/trip_summary"
    claims = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/bronze/claims"

    tlm = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/silver/telematics"
    clm_silver = "hdfs://localhost:9000/user/dr.who/ingestion_area/current/silver/claims"

# ADDING CURRENT INFORMATIONS IN THE TELEMATIC DW
    postgres_url = "jdbc:postgresql://localhost:5432/telematic_dw"
    db_user = "postgres"
    db_password = "root"

    feed_behaviour_tables(spark, behaviour, postgres_url, db_user, db_password)
    feed_summary_tables(spark, trip_summary, postgres_url, db_user, db_password)
    feed_silver_tables(spark, tlm, postgres_url, db_user, db_password)
    feed_hive_tables()

# BACKUP
    access_key = "AKIA4HS5FRFJKYG77C2D"
    secret_key = "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"
    bucket_name = "telematicbackup"
    backup_bronze(claims, access_key, secret_key, bucket_name, "claims")
    backup_bronze(province, access_key, secret_key, bucket_name, "province")
    backup_bronze(trip_summary, access_key, secret_key, bucket_name, "trip_summary")
    backup_bronze(behaviour, access_key, secret_key, bucket_name, "behaviour")

# HISTORICAL PATHS
    behaviour_hist = "hdfs://localhost:9000/user/dr.who/ingestion_area/historical/bronze/telematics/behaviour"
    province_hist = "hdfs://localhost:9000/user/dr.who/ingestion_area/historical/bronze/telematics/province"
    trip_summary_hist = "hdfs://localhost:9000/user/dr.who/ingestion_area/historical/bronze/telematics/trip_summary"
    claims_hist = "hdfs://localhost:9000/user/dr.who/ingestion_area/historical/bronze/claims"

    tlm_hist = "hdfs://localhost:9000/user/dr.who/ingestion_area/historical/silver/telematics"
    clm_silver_hist = "hdfs://localhost:9000/user/dr.who/ingestion_area/historical/silver/claims"

# MOVE THE FILES IN HISTORICAL
    feed_historical_with_current(spark, behaviour, behaviour_hist)
    feed_historical_with_current(spark, province, province_hist)
    feed_historical_with_current(spark, trip_summary, trip_summary_hist)
    feed_historical_with_current(spark, claims, claims_hist)
    feed_historical_with_current(spark, tlm, tlm_hist)
    feed_historical_with_current(spark, clm_silver, clm_silver_hist)

spark.stop()

