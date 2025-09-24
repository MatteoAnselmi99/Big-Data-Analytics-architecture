import os, logging, sys, torch

import pandas as pd

from BDA_Scripts.print_to_log import StreamToLogger

os.environ["CUDA_LAUNCH_BLOCKING"] = "1"

from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler, VectorAssembler, StandardScaler
from pyspark.sql.functions import when, avg
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import mean, stddev, col

import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import TensorDataset, DataLoader

import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from datetime import datetime

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

def create_save_plot(RMSE_Loss, timestamp):
    # PLOT CREATION
    plt.figure()
    plt.plot(RMSE_Loss)
    plt.grid(True)
    plt.xlabel("Epoch")
    plt.ylabel("RMSE Loss")
    plt.title("Incurred Training Loss Curve")

    # PLOT SAVE
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"training_loss_{timestamp}.png")
    plt.savefig(output_path)
    print(f"Plot saved in: {output_path}")

# ======================================== PRE-PROCESSING
def normalization(df):

    # NEW FEATURES CREATION
    df = df.withColumn("perc_curve",
                       when((col("meters_urban") + col("meters_other")) != 0,
                            col("cornering") / (col("meters_urban") + col("meters_other"))).otherwise(0))

    df = df.withColumn("perc_change_line",
                       when((col("meters_motorway") + col("meters_urban")) != 0,
                            col("lateral_movement") / (col("meters_motorway") + col("meters_urban"))).otherwise(0))

    df = df.withColumn("avg_speed_motorway",
                       when(col("seconds_motorway") != 0,
                            col("meters_motorway") / col("seconds_motorway")).otherwise(0))

    df = df.withColumn("avg_speed_urban",
                       when(col("seconds_urban") != 0,
                            col("meters_urban") / col("seconds_urban")).otherwise(0))

    df = df.withColumn("avg_speed_other",
                       when(col("seconds_other") != 0,
                            col("meters_other") / col("seconds_other")).otherwise(0))

    df = df.withColumn("avg_speed_monday", when(col("seconds_travelled_monday") != 0,
                                                col("meters_travelled_monday") / col(
                                                    "seconds_travelled_monday")).otherwise(0))

    df = df.withColumn("avg_speed_tuesday", when(col("seconds_travelled_tuesday") != 0,
                                                 col("meters_travelled_tuesday") / col(
                                                     "seconds_travelled_tuesday")).otherwise(0))

    df = df.withColumn("avg_speed_wednesday", when(col("seconds_travelled_wednesday") != 0,
                                                   col("meters_travelled_wednesday") / col(
                                                       "seconds_travelled_wednesday")).otherwise(0))

    df = df.withColumn("avg_speed_thursday", when(col("seconds_travelled_thursday") != 0,
                                                  col("meters_travelled_thursday") / col(
                                                      "seconds_travelled_thursday")).otherwise(0))

    df = df.withColumn("avg_speed_friday", when(col("seconds_travelled_friday") != 0,
                                                col("meters_travelled_friday") / col(
                                                    "seconds_travelled_friday")).otherwise(0))

    df = df.withColumn("avg_speed_saturday", when(col("seconds_travelled_saturday") != 0,
                                                  col("meters_travelled_saturday") / col(
                                                      "seconds_travelled_saturday")).otherwise(0))

    df = df.withColumn("avg_speed_sunday", when(col("seconds_travelled_sunday") != 0,
                                                col("meters_travelled_sunday") / col(
                                                    "seconds_travelled_sunday")).otherwise(0))

    df = df.withColumn("acc_motorway",
                       when(col("total_meters") != 0,
                            col("acceleration") * (col("meters_motorway") / col("total_meters"))).otherwise(0))

    df = df.withColumn("brake_motorway",
                       when(col("total_meters") != 0,
                            col("brake") * (col("meters_motorway") / col("total_meters"))).otherwise(0))

    df = df.withColumn("avg_speed_day",
                       when(col("seconds_travelled_day") != 0,
                            col("meters_travelled_day") / col("seconds_travelled_day")).otherwise(0))

    df = df.withColumn("avg_speed_night",
                       when(col("seconds_travelled_night") != 0,
                            col("meters_travelled_night") / col("seconds_travelled_night")).otherwise(0))

    df = df.withColumn("avg_duration",
                       when(col("number_trips") != 0,col("duration") / col("number_trips")).otherwise(0))

    # MINMAX_SCALING
    cols_to_scale = ['total_meters', 'total_seconds', 'speeding', 'acceleration', 'brake', 'cornering','lateral_movement',
                     'avg_speed_motorway', 'avg_speed_urban', 'avg_speed_other', 'avg_speed_monday', 'avg_speed_tuesday',
                     'avg_speed_wednesday', 'avg_speed_thursday', 'avg_speed_friday', 'avg_speed_saturday',
                     'avg_speed_sunday','brake_motorway','acc_motorway', 'avg_speed_day', 'avg_speed_night', 'avg_duration']

    assembler = VectorAssembler(inputCols=cols_to_scale, outputCol="features_vec")
    scaler = StandardScaler(inputCol="features_vec", outputCol="scaled_features", withMean=True, withStd=True)
    pipeline = Pipeline(stages=[assembler, scaler])
    scaler_model = pipeline.fit(df)
    df = scaler_model.transform(df)

    # NORMALIZED COLUMNS OVERWRITING
    df = df.withColumn("scaled_array", vector_to_array("scaled_features"))
    for i, col_name in enumerate(cols_to_scale):
        df = df.withColumn(col_name, col("scaled_array")[i])

    # REMOVE TEMPORARY COLUMNS
    df = df.drop("features_vec", "scaled_features", "scaled_array")

    # CLAIMS BINARIZATION
    binary_cols = ['risk_1', 'risk_2', 'risk_3', 'risk_4', 'risk_5', 'risk_6','body_injuries', 'property_damage']
    for c in binary_cols:
        df = df.withColumn(c, when(col(c) != 0, 1).otherwise(0))

    # CLEANING
    df = df.fillna(0)
    df = df.dropDuplicates()

    # CLAIMS WITH FAULT AGGREGATION ([cardC, cardD] --> RCA)
    df = df.withColumn("rca", when(col("cardD") != 0, col("cardD")).otherwise(col("rca")))
    df = df.withColumn("rca", when(col("cardC") != 0, col("cardC")).otherwise(col("rca")))
    df = df.withColumn("rca", when(col("rca") != 0, 1).otherwise(0))
    df = df.filter(col('rca') > 0)

    # "INCURRED" COLUMN GAUSSIAN STANDARDIZATION
    mean_incurred = df.select(mean(col("incurred"))).collect()[0][0]
    std_incurred = df.select(stddev(col("incurred"))).collect()[0][0]
    df = df.withColumn("incurred", (col("incurred") - mean_incurred) / std_incurred)

    return df
#======================== DL MODEL
class Incurred_DL(nn.Module):
    def __init__(self, input_dim=23):
        super(Incurred_DL, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.3),

            nn.Linear(128, 64),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(0.3),

            nn.Linear(64, 32),
            nn.ReLU(),

            nn.Linear(32, 1)  # output continuo
        )

    def forward(self, x):
        return self.model(x)



def rmse_loss(y_pred, y_true):
    return torch.sqrt(F.mse_loss(y_pred, y_true))



def evaluate_model(model, X_test, y_test, output_dir, timestamp, criterion, device):
    X_test = X_test.to(device)
    y_test = y_test.to(device)

    model.eval()
    with torch.no_grad():
        test_outputs = model(X_test)
        test_loss = criterion(test_outputs, y_test).item()

    # .txt BUILDING
    report = []
    report.append(f"RMSE Loss: {test_loss:.4f}")
    report_text = "\n".join(report)
    file_path = os.path.join(output_dir, f"Incurred_RMSE_Loss_{timestamp}.txt")

    with open(file_path, "w") as f:
        f.write(report_text)

    print(f"Report saved in: {file_path}")
    print(f"\nRMSE on test set: {test_loss:.4f}")


#=============================================== MODEL TRAINING
def train_incurred_model(num_epochs, learning_rate, X_train, y_train, device, rmse_loss):
    X_train = X_train.to(device)
    y_train = y_train.to(device)

    # MODEL BUILDING
    model = Incurred_DL(input_dim=24).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)

    # TRAINING
    rmse_losses = []

    try:
        for epoch in range(num_epochs):
            model.train()
            outputs = model(X_train)
            loss = rmse_loss(outputs, y_train)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            avg_loss = loss.item()
            rmse_losses.append(avg_loss)

            if (epoch + 1) % 50 == 0:
                print(f"Epoch [{epoch + 1}/{num_epochs}] - Loss: {avg_loss:.4f}")

    except Exception as e:
        print("Error during the training:", e)

    model.eval()
    with torch.no_grad():
        test_outputs = model(X_train)
        test_loss = criterion(test_outputs, y_train).item()
    print((f"RMSE on training set: {test_loss:.4f}"))

    return rmse_losses, model

#========================== X/Y, TRAIN/TEST SPLITTING
def set_train_test(pandas_df):
    # DOMAIN SET
    df_x = pandas_df.drop(columns=['atti_vandalici', 'cardC', 'cardD', 'casco',
                                   'collision', 'glass', 'natural_events', 'maindriver_injuries', 'fire_theft', 'rca',
                                   'incurred','meters_motorway', 'meters_urban', 'meters_other', 'meters_travelled_monday',
                                   'meters_travelled_tuesday', 'meters_travelled_wednesday', 'meters_travelled_thursday',
                                   'meters_travelled_friday', 'meters_travelled_saturday', 'meters_travelled_sunday',
                                   'meters_travelled_day', 'meters_travelled_night',
                                   'seconds_travelled_monday','seconds_travelled_tuesday',
                                   'seconds_travelled_wednesday', 'seconds_travelled_thursday','seconds_travelled_friday',
                                   'seconds_travelled_day', 'speeding', 'acceleration', 'brake',
                                   'seconds_motorway', 'seconds_urban', 'seconds_other',
                                   'seconds_travelled_night','seconds_travelled_saturday', 'seconds_travelled_sunday',
                                   'speeding','acceleration','brake','cornering','lateral_movement','duration',
                                   'number_trips', 'body_injuries','property_damage','body_and_property_damage',
                                   'ingestion_date', 'contract', 'total_seconds'])
    # LABEL SET
    df_y = pandas_df['incurred']

    pd.set_option('display.max_columns', None)
    print(list(df_x.columns))
    print(df_x.head())

    # SPLITTING TRAIN / TEST (80% / 20%)
    X_train, X_test, y_train, y_test = train_test_split(df_x, df_y, test_size=0.2, random_state=42)
    X_train_tensor = torch.tensor(X_train.values, dtype=torch.float32)
    y_train_tensor = torch.tensor(y_train.values, dtype=torch.float32).unsqueeze(1)
    X_test_tensor = torch.tensor(X_test.values, dtype=torch.float32)
    y_test_tensor = torch.tensor(y_test.values, dtype=torch.float32).unsqueeze(1)

    return X_train_tensor, X_test_tensor, y_train_tensor, y_test_tensor

# ========================== LOGS
app_name = "Incurred_model"
log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/10_incurred_model"
log4j_path = set_spark_python_logs(app_name, log_base_dir)
#=========================================================== SPARK SESSION
spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.mergeSchema", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.debug.maxToStringFields", 100) \
        .config("spark.sql.hive.convertMetastoreParquet", "true") \
        .config("spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_path}") \
        .config("spark.executor.extraJavaOptions", f"-Dlog4j.configuration=file:{log4j_path}") \
    .getOrCreate()

df = spark.read.parquet("hdfs://localhost:9000/user/dr.who/ingestion_area/historical/gold")

df = normalization(df)
pandas_df = df.toPandas()
spark.stop()

X_train, X_test, y_train, y_test = set_train_test(pandas_df)

# PARAMETERS
num_epochs = 100
learning_rate = 0.001
criterion = rmse_loss

# MOVE THE WORKLOAD ON GPU (NVIDIA RTX 4060)
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

rmse_losses, model = train_incurred_model(num_epochs, learning_rate, X_train, y_train, device, criterion)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = f"./model_validations/Incurred_{timestamp}"
create_save_plot(rmse_losses, timestamp)

evaluate_model(model, X_test, y_test, output_dir, timestamp, criterion, device)

# SAVE MODEL WEIGHTS
torch.save(model, f"./model_knowledge/Incurred/_{timestamp}_.pth")
print(f"Model saved in ./model_knowledge/Incurred/_{timestamp}_.pth")
torch.cuda.empty_cache()