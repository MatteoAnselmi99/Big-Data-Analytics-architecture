import os, logging, sys
import pandas as pd
from BDA_Scripts.print_to_log import StreamToLogger
os.environ["CUDA_LAUNCH_BLOCKING"] = "1"
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import when, col
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array
from datetime import datetime
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.metrics import precision_score, recall_score
import matplotlib.pyplot as plt

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
                       when(col("number_trips") != 0, col("duration") / col("number_trips")).otherwise(0))

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
    binary_cols = [
        'risk_1', 'risk_2', 'risk_3', 'risk_4', 'risk_5', 'risk_6',
        'body_injuries', 'property_damage'
    ]
    for c in binary_cols:
        df = df.withColumn(c, when(col(c) != 0, 1).otherwise(0))

    # CLEANING
    df = df.fillna(0)
    df = df.dropDuplicates()

    # CLAIMS WITH FAULT AGGREGATION ([cardC, cardD] --> RCA)
    df = df.withColumn("rca", when(col("cardD") != 0, col("cardD")).otherwise(col("rca")))
    df = df.withColumn("rca", when(col("cardC") != 0, col("cardC")).otherwise(col("rca")))
    df = df.withColumn("rca", when(col("rca") != 0, 1).otherwise(0))

    return df

#========================== X/Y, TRAIN/TEST SPLITTING
def set_train_test(pandas_df):
    # SPLITTING TRAIN / TEST
    df_train_zero_b = pandas_df[pandas_df['body_injuries'] == 0].sample(n=3500, random_state=42)
    df_train_one_b = pandas_df[pandas_df['body_injuries'] == 1].sample(n=350, random_state=42)
    df_train_zero_p = pandas_df[pandas_df['property_damage'] == 0].sample(n=3500, random_state=42)
    df_train_one_p = pandas_df[pandas_df['property_damage'] == 1].sample(n=3500, random_state=42)
    df_train = pd.concat([df_train_one_b, df_train_zero_b, df_train_zero_p, df_train_one_p])

    # REMOVE THE ROWS USED IN TRAINING TEST FROM THE DATASET
    pandas_df = pandas_df.drop(df_train.index)

    # RESET INDEX
    pandas_df = pandas_df.reset_index(drop=True)

    # CREATE TEST DATASET
    df_test_zero_b = pandas_df[pandas_df['body_injuries'] == 0].sample(n=1000, random_state=42)
    df_test_one_b = pandas_df[pandas_df['body_injuries'] == 1].sample(n=227, random_state=42)
    df_test_zero_p = pandas_df[pandas_df['property_damage'] == 0].sample(n=1000, random_state=42)
    df_test_one_p = pandas_df[pandas_df['property_damage'] == 1].sample(n=1000, random_state=42)

    df_test = pd.concat([df_test_one_p, df_test_zero_p, df_test_zero_b, df_test_one_b])

    # ================ TRAINING
    # DOMAIN SET
    df_x_train = df_train.drop(columns=['atti_vandalici', 'cardC', 'cardD', 'casco',
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
                                   'ingestion_date', 'contract','total_seconds'], axis=1)
    # LABEL SET
    df_y_train = df_train[['body_injuries','property_damage']]

    # ================= TEST
    # DOMAIN SET
    df_x_test = df_test.drop(columns=['atti_vandalici', 'cardC', 'cardD', 'casco',
                                        'collision', 'glass', 'natural_events', 'maindriver_injuries', 'fire_theft',
                                        'rca',
                                        'incurred', 'meters_motorway', 'meters_urban', 'meters_other',
                                        'meters_travelled_monday',
                                        'meters_travelled_tuesday', 'meters_travelled_wednesday',
                                        'meters_travelled_thursday',
                                        'meters_travelled_friday', 'meters_travelled_saturday',
                                        'meters_travelled_sunday',
                                        'meters_travelled_day', 'meters_travelled_night',
                                        'seconds_travelled_monday', 'seconds_travelled_tuesday',
                                        'seconds_travelled_wednesday', 'seconds_travelled_thursday',
                                        'seconds_travelled_friday',
                                        'seconds_travelled_day', 'speeding', 'acceleration', 'brake',
                                        'seconds_motorway', 'seconds_urban', 'seconds_other',
                                        'seconds_travelled_night', 'seconds_travelled_saturday',
                                        'seconds_travelled_sunday',
                                        'speeding', 'acceleration', 'brake', 'cornering', 'lateral_movement',
                                        'duration',
                                        'number_trips', 'body_injuries', 'property_damage', 'body_and_property_damage',
                                        'ingestion_date', 'contract', 'total_seconds'], axis=1)
    # LABEL SET
    df_y_test = df_test[['body_injuries', 'property_damage']]

    #X_train, y_train, X_test, y_test = iterative_train_test_split(df_x.to_numpy(), df_y.to_numpy(), test_size=0.35)
    X_train = torch.tensor(df_x_train.values, dtype=torch.float32)
    y_train = torch.tensor(df_y_train.values, dtype=torch.float32)
    X_test = torch.tensor(df_x_test.values, dtype=torch.float32)
    y_test = torch.tensor(df_y_test.values, dtype=torch.float32)

    # ENSURE Y SHAPE = (n, 1)
    if y_train.ndim == 1:
        y_train = y_train.unsqueeze(1)
    if y_test.ndim == 1:
        y_test = y_test.unsqueeze(1)

    return X_train, X_test, y_train, y_test

class DeepMultiLabelNN(nn.Module):
    def __init__(self, input_dim, output_dim):
        super(DeepMultiLabelNN, self).__init__()
        self.net = nn.Sequential(
            nn.Linear(input_dim, 512),
            nn.GELU(),
            nn.Dropout(0.4),

            nn.Linear(512, 128),
            nn.GELU(),
            nn.Dropout(0.3),

            nn.Linear(128, 64),
            nn.GELU(),
            nn.Dropout(0.2),

            nn.Linear(64, 32),
            nn.GELU(),
            nn.Dropout(0.1),

            nn.Linear(32, output_dim)  # output_dim = 2
        )

    def forward(self, x):
        return self.net(x)

def create_save_plot(loss_history, timestamp, output_dir):
    # PLOT CREATION
    plt.figure()
    plt.plot(loss_history)
    plt.xlabel("Epoch")
    plt.ylabel("Loss")
    plt.title("Claim Type Loss Curve")

    # PLOT SAVE
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"training_loss_{timestamp}.png")
    plt.savefig(output_path)

    print(f"Plot salvato in: {output_path}")

# ========================== LOGS
app_name = "CLM_type_model"
log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/11_CLM_type_model"
log4j_path = set_spark_python_logs(app_name, log_base_dir)

def evaluation(model, X_test, y_test, output_dir, timestamp, device):
    model = model.to(device)
    X_test = X_test.to(device)
    y_test = y_test.to(device)

    # EVALUATION
    model.eval()

    with torch.no_grad():
        outputs = model(X_test)
        probs = torch.sigmoid(outputs)
        preds = (probs > 0.5).int()

    y_test = y_test.cpu().numpy()
    preds = preds.cpu().numpy()
    print("Test set evaluation :")

    label_names = ["property_damage", "body_injuries"]
    precisions = precision_score(y_test, preds, average=None)
    for name, prec in zip(label_names, precisions):
        print(f"Precision {name}: {prec:.4f}")

    recalls = recall_score(y_test, preds, average=None)
    for name, prec in zip(label_names, recalls):
        print(f"Recall {name}: {prec:.4f}")

    # .txt BUILDING
    report = []
    report.append(f"Precisions : {precisions}")
    report.append(f"Recalls : {recalls}")
    report_text = "\n".join(report)
    file_path = os.path.join(output_dir, f"CLM_Type_report_{timestamp}.txt")

    with open(file_path, "w") as f:
        f.write(report_text)

    print(f"Report saved in: {file_path}")


def train_incurred_model(num_epochs, learning_rate, X_train, y_train, device, criterion, train_losses):
    X_train = X_train.to(device)
    y_train = y_train.to(device)

    # MODEL CREATION
    model = DeepMultiLabelNN(input_dim=24, output_dim=2).to(device)

    # LOSS AND OPTIMIZER
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    for epoch in range(num_epochs):
        model.train()
        optimizer.zero_grad()
        output = model(X_train)
        loss = criterion(output, y_train)
        loss.backward()
        optimizer.step()
        train_losses.append(loss.item())

        if (epoch + 1) % 10 == 0:
            print(f"Epoch [{epoch + 1}/{num_epochs}] - Loss: {loss.item():.4f}")

    # TRAINING EVALUATION
    model.eval()

    with torch.no_grad():
        outputs = model(X_train)
        probs = torch.sigmoid(outputs)
        preds = (probs > 0.5).int()

    y_train = y_train.cpu().numpy()
    preds = preds.cpu().numpy()
    print("Training set evaluation : ")

    label_names = ["property_damage", "body_injuries"]
    precisions = precision_score(y_train, preds, average=None)
    for name, prec in zip(label_names, precisions):
        print(f"Precision {name}: {prec:.4f}")

    recalls = recall_score(y_train, preds, average=None)
    for name, prec in zip(label_names, recalls):
        print(f"Recall {name}: {prec:.4f}")

    return train_losses, model

# ========================== LOGS
app_name = "CLM_type_model"
log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/11_CLM_type_model"
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

# MOVE THE WORKLOAD ON GPU (NVIDIA RTX 4060)
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

num_epochs = 1000
learning_rate = 0.01
train_losses = []
criterion = nn.BCEWithLogitsLoss()
train_losses, model = train_incurred_model(num_epochs, learning_rate, X_train, y_train, device, criterion, train_losses)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = f"./model_validations/CLM_type_{timestamp}"
create_save_plot(train_losses, timestamp, output_dir)

evaluation(model, X_test, y_test, output_dir, timestamp, device)

# SAVE MODEL WEIGHTS
torch.save(model, f"./model_knowledge/clm_type/_{timestamp}_.pth")
print(f"Model saved in ./model_knowledge/clm_type/_{timestamp}_.pth")
torch.cuda.empty_cache()
