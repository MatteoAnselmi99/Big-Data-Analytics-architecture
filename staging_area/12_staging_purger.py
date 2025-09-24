import subprocess
import sys, os, logging
from BDA_Scripts.print_to_log import StreamToLogger

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

def remove_csv_files(source_dir_str):
    files_csv = list_csv_files_hdfs(source_dir_str)

    for file_path in files_csv:
        file_name = os.path.basename(file_path)

        cmd = ["/opt/hadoop/bin/hdfs", "dfs", "-rm", file_path]
        subprocess.run(cmd, check=True)
        print(f"Removed : {file_name} in {file_path}")

if __name__ == "__main__":
    # PYTHON LOGS
    app_name = "staging_purger"
    log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/12_staging_purger"
    set_python_logs(app_name, log_base_dir)

    remove_csv_files("hdfs://localhost:9000/user/dr.who/staging_area/ingested/telematics/behaviour")
    remove_csv_files("hdfs://localhost:9000/user/dr.who/staging_area/ingested/telematics/province")
    remove_csv_files("hdfs://localhost:9000/user/dr.who/staging_area/ingested/telematics/trip_summary")
    remove_csv_files("hdfs://localhost:9000/user/dr.who/staging_area/ingested/claims")