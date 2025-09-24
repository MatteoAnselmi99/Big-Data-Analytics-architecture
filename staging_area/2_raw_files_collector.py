import os, logging, sys, subprocess
from pathlib import Path
from BDA_Scripts.print_to_log import StreamToLogger

# LOGS
def set_python_logs(app_name, log_base_dir):
    log_dir = os.path.join(log_base_dir, app_name)
    os.makedirs(log_dir, exist_ok=True)
    log4p_path = os.path.join(log_dir, "python.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[logging.FileHandler(log4p_path)])
    logger = logging.getLogger(__name__)
    sys.stdout = StreamToLogger(logger, logging.INFO)
    sys.stderr = StreamToLogger(logger, logging.ERROR)

# MOVE FROM DOWNLOAD FOLDER TO HDFS
def move_csv_files(source_dir_str, dest_dir_str):
    source_dir = Path(source_dir_str)

    # CHECK CSV EXISTS
    csv_files = list(source_dir.glob('*.csv'))
    if not csv_files:
        print(f"No CSV found in : {source_dir}")
        return

    for file_path in csv_files:
        try:
            dest = os.path.join(dest_dir_str, file_path.name)

            cmd = ["/opt/hadoop/bin/hdfs", "dfs", "-put", "-f", str(file_path), dest]
            subprocess.run(cmd, check=True)
            print(f"{file_path.name} loaded in HDFS : {dest}")

            # REMOVE FILE FROM THE "AVAILABLE" SERVER SIDE
            file_path.unlink()
            print(f"Removed local file : {file_path.name}")
        except subprocess.CalledProcessError as e:
            print(f"HDFS Error : {file_path.name} → {e}")
        except Exception as e:
            print(f"Generic error moving {file_path.name} → {e}")


if __name__ == '__main__':
    app_name = "raw_files_collector"
    log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/2_raw_files_collector"
    set_python_logs(app_name, log_base_dir)

    move_csv_files('/home/matteo/server_locale/provider_csv/available/claims',
                   '/user/dr.who/staging_area/incoming/claims')

    move_csv_files('/home/matteo/server_locale/provider_csv/available/telematics/behaviour',
                   '/user/dr.who/staging_area/incoming/telematics/behaviour')

    move_csv_files('/home/matteo/server_locale/provider_csv/available/telematics/province',
                   '/user/dr.who/staging_area/incoming/telematics/province')

    move_csv_files('/home/matteo/server_locale/provider_csv/available/telematics/trip_summary',
                   '/user/dr.who/staging_area/incoming/telematics/trip_summary')