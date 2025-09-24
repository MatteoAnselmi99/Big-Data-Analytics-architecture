import csv, os, logging, sys
import subprocess
from pathlib import Path
from BDA_Scripts.print_to_log import StreamToLogger

# PYTHON LOGS
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

def validate_csv_structure(hdfs_path, local_path) -> bool:
    try:
        # DOWNLOAD IN A CACHE THE CSV FILE IN ORDER TO OPEN IT
        subprocess.run(['/opt/hadoop/bin/hdfs', 'dfs', '-get', '-f', hdfs_path, local_path], check=True)
        with local_path.open(newline='', encoding='utf-8') as f:

            # CLAIMS CSV USES ; AS DELIMITER
            if 'CLAIMS' in str(local_path):
                reader = csv.reader(f, delimiter=';')
            else :
                reader = csv.reader(f, delimiter=',')
            rows = list(reader)

        if not rows:
            print(f"The file is empty : {local_path.name}")
            return False

        # HEADER ANALYSIS
        header = rows[0]
        n_columns = len(header)
        print(f"Header has {n_columns} columns : {header}")

        # CHECK N_COLUMNS FOR EACH ROW
        for i, row in enumerate(rows[1:], start=2):  # The first row is skipped because it's the header
            if len(row) != n_columns:
                print(f"Row {i} has {len(row)} columns (expected: {n_columns}): {row}")
                return False

        print(f"The file : '{local_path.name}' is validated.")
        return True

    except Exception as e:
        print(f"Error reading the file : {e}")
        return False

    # DELETE FILE IN CACHE
    finally:
        if local_path.exists():
            local_path.unlink()

# LIST ALL THE OBJECT IN THE FOLDER
def list_csv_files_hdfs(hdfs_folder):
    files = []
    cmd = ['/opt/hadoop/bin/hdfs', 'dfs', '-ls', hdfs_folder]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    for line in result.stdout.splitlines():
        parts = line.split()
        if parts and parts[-1].endswith('.csv'): # THE LAST ELEMENT IS THE PATH
            files.append(parts[-1])
    return files

# FILE MOVER
def start_validation(path_folder, valid_folder, error_folder):
    files_csv = list_csv_files_hdfs(path_folder)

    for file_path in files_csv:
        file_name = os.path.basename(file_path)
        local_path = Path(os.path.join("/home/matteo/BDA_project/staging_area/incoming", file_name))
        print(f"Check file : {file_name}")
        try:
            # CHECK THE STRUCTURE
            check_result = validate_csv_structure(file_path, local_path)

            # IF CSV STRUCTURE OK, GO IN VALID FOLDER, ELSE GO IN ERROR FOLDER
            if check_result:
                dest = os.path.join(valid_folder, file_name)
            else:
                dest = os.path.join(error_folder, file_name)

            cmd = ["/opt/hadoop/bin/hdfs", "dfs", "-mv", file_path, dest]
            subprocess.run(cmd, check=True)
            print(f"Moved: {file_name} in {dest}")

        # IF EXCEPTION, CSV GO IN ERROR FOLDER
        except Exception as e:
            print(f"Error before checking the file {file_name} : {e}")
            dest = os.path.join(error_folder, file_name)
            cmd = ["/opt/hadoop/bin/hdfs", "dfs", "-mv", file_path, dest]
            subprocess.run(cmd, check=True)
            print(f"Moved: {file_name} in {dest}")

if __name__ == '__main__':
    app_name = "file_quality_checker"
    log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/3_file_quality_checker"
    set_python_logs(app_name, log_base_dir)

    start_validation('/user/dr.who/staging_area/incoming/claims',
                     '/user/dr.who/staging_area/approved/claims',
                     '/user/dr.who/staging_area/rejected/claims')

    start_validation('/user/dr.who/staging_area/incoming/telematics/behaviour',
                     '/user/dr.who/staging_area/approved/telematics/behaviour',
                     '/user/dr.who/staging_area/rejected/telematics/behaviour')

    start_validation('/user/dr.who/staging_area/incoming/telematics/province',
                     '/user/dr.who/staging_area/approved/telematics/province',
                     '/user/dr.who/staging_area/rejected/telematics/province')

    start_validation('/user/dr.who/staging_area/incoming/telematics/trip_summary',
                     '/user/dr.who/staging_area/approved/telematics/trip_summary',
                     '/user/dr.who/staging_area/rejected/telematics/trip_summary')