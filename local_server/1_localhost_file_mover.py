import os, shutil, logging, sys, random
from BDA_Scripts.print_to_log import StreamToLogger
from pathlib import Path

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

def move_csv_files(source_dir_str, dest_dir_str):
    source_dir = Path(source_dir_str)
    dest_dir = Path(dest_dir_str)

    # LIST OF CSV FILES IN SOURCE_DIR
    csv_files = sorted(source_dir.glob("*.csv"))

    # Se almeno uno esiste, copiane uno
    if csv_files:
        x = random.randint(1, 10)
        file_path = csv_files[x]
        dest_path = dest_dir / file_path.name
        try:
            shutil.move(str(file_path), str(dest_path))
            print(f"Moved: {file_path.name}")
        except Exception as e:
            print(f"Error moving {file_path.name}: {e}")
    else:
        print("No files found.")

if __name__ == "__main__":
    # PYTHON LOGS
    app_name = "localhost_file_mover"
    log_base_dir = "/home/matteo/PycharmProjects/Tesi/BDA_Scripts/logs/1_localhost_file_mover"
    set_python_logs(app_name, log_base_dir)

    move_csv_files("/home/matteo/server_locale/provider_csv/not_available/telematics/behaviour",
                   "/home/matteo/server_locale/provider_csv/available/telematics/behaviour")

    move_csv_files("/home/matteo/server_locale/provider_csv/not_available/telematics/province",
                   "/home/matteo/server_locale/provider_csv/available/telematics/province")

    move_csv_files("/home/matteo/server_locale/provider_csv/not_available/telematics/trip_summary",
                   "/home/matteo/server_locale/provider_csv/available/telematics/trip_summary")

    move_csv_files("/home/matteo/server_locale/provider_csv/not_available/claims",
                   "/home/matteo/server_locale/provider_csv/available/claims")

