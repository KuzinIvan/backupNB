import requests
import os
import argparse
import base64
import configparser
import time
from datetime import datetime
from croniter import croniter
import zipfile
import re
import tempfile
import logging
import sys

parser = argparse.ArgumentParser(description='NextBox Backup Utility')
parser.add_argument('--config', type=str, default="config.ini", help='Path to config file')
args = parser.parse_args()

config = configparser.ConfigParser()
config.read(args.config)

cron_schedule = config.get('backup', 'cron_schedule')
local_dir = config.get('backup', 'local_dir')
nextbox_dir = config.get('backup', 'nextbox_dir')
divide_id = config.get('backup', 'divide_id')
rotation_count = config.getint('backup', 'rotation_count')
nextbox_host = config.get('backup', 'nextbox_host')
nextbox_username = config.get('backup', 'nextbox_username')
nextbox_password = config.get('backup', 'nextbox_password')

auth_string = base64.b64encode(f"{nextbox_username}:{nextbox_password}".encode()).decode("utf-8")


def request_header():
    return {
        "Accept-Language": "ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7",
        "Authorization": f"Basic {str(auth_string)}"
    }


def delete_nextbox_file(full_path):
    params = {'path': full_path}
    if divide_id != 0:
        params['divide_id'] = int(divide_id)
    headers = request_header()
    response = requests.delete(nextbox_host + "/storage/element", headers=headers, params=params)
    response.raise_for_status()


def create_directory(nb_dir, div_id):
    """Создает директорию в NextBox"""
    name = nb_dir.split("/")[-1]
    path = '/' + '/'.join(nb_dir.split("/")[:-1])


    payload = {
        "name": name,
        "type": "dir",
        "path": path,
        "is_work_dir": False
    }
    if divide_id != 0:
        payload["divide_id"] = int(div_id)
    headers = request_header()
    headers["Content-Type"] = "application/json"

    try:
        response = requests.post(
            nextbox_host + "/storage/element",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        logging.info(f"Directory created: {path}")
        return True
    except Exception as e:
        logging.error(f"Failed to create directory {path}: {str(e)}")
        return False


def list_nextbox_dir(path, search=None):
    """Получает список файлов в директории NextBox, создает директорию при 404"""
    params = {"path": path, "offset": 0, "limit": 1000}
    if divide_id != 0:
        params["divide_id"] = int(divide_id)
    if search:
        params["search"] = search

    headers = request_header()

    try:
        response = requests.get(nextbox_host + "/storage", headers=headers, params=params)
        response.raise_for_status()
        return response.json()["rows"]

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            # Попробуем создать директорию
            if create_directory(nextbox_dir, divide_id):
                # Повторяем запрос после создания директории
                response = requests.get(nextbox_host + "/storage", headers=headers, params=params)
                response.raise_for_status()
                return response.json()["rows"]
            else:
                # Если не удалось создать директорию, бросаем исключение
                raise Exception(f"Failed to create directory {path}") from e
        else:
            # Для других HTTP ошибок просто бросаем исключение
            raise


def upload_file(file_path, target_dir):
    file_name = os.path.basename(file_path)
    headers = request_header()
    headers.pop("Content-Type", None)

    with open(file_path, 'rb') as f:
        files = {'file': (file_name, f)}
        data = {'path': target_dir}
        if divide_id != 0:
            data["divide_id"] = int(divide_id)
        response = requests.post(
            nextbox_host + "/storage/files",
            headers=headers,
            files=files,
            data=data
        )
    response.raise_for_status()


def create_backup_archive():
    dir_name = os.path.basename(os.path.normpath(local_dir))
    timestamp = datetime.now().strftime("%d.%m.%Y_%H_%M_%S")
    archive_name = f"{dir_name}_{timestamp}.zip"
    archive_path = os.path.join(tempfile.gettempdir(), archive_name)

    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(local_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, local_dir)
                zipf.write(file_path, arcname)

    return archive_path


def rotate_backups():
    if rotation_count <= 0:
        return

    try:
        dir_name = os.path.basename(os.path.normpath(local_dir))

        all_files = list_nextbox_dir(nextbox_dir, dir_name)

        backup_files = []
        for item in all_files:
            if item['type'] != 'file':
                continue
            if not item['name'].startswith(dir_name):
                continue
            if not item['name'].endswith('.zip'):
                continue

            match = re.search(r'(\d{2}\.\d{2}\.\d{4}_\d{2}_\d{2}_\d{2})', item['name'])
            if match:
                try:
                    file_date = datetime.strptime(match.group(1), "%d.%m.%Y_%H_%M_%S")
                    backup_files.append({
                        'name': item['name'],
                        'date': file_date,
                        'full_path': item['full_path']
                    })
                except:
                    continue

        if len(backup_files) <= rotation_count:
            logging.info(
                f"Backup rotation: {len(backup_files)} backups found, limit is {rotation_count} - no deletion needed")
            return

        backup_files.sort(key=lambda x: x['date'], reverse=True)

        files_to_delete = backup_files[rotation_count:]

        for file_info in files_to_delete:
            try:
                delete_nextbox_file(file_info['full_path'])
                logging.info(f"Deleted old backup: {file_info['name']}")
            except Exception as e:
                logging.error(f"Failed to delete {file_info['name']}: {str(e)}")

        logging.info(f"Backup rotation completed: deleted {len(files_to_delete)} old backups")

    except Exception as e:
        logging.error(f"Backup rotation failed: {str(e)}")


def perform_backup():
    logging.info(f"[{datetime.now()}] Starting backup process...")
    archive_path = None
    try:
        rotate_backups()
    except Exception as e:
        logging.error(f"Rotation error: {str(e)}")

    try:
        archive_path = create_backup_archive()
        logging.info(f"Backup archive created: {archive_path}")

        upload_file(archive_path, nextbox_dir)
        logging.info(f"[{datetime.now()}] Backup uploaded successfully")

        try:
            os.remove(archive_path)
            logging.info(f"Temporary file removed: {archive_path}")
        except Exception as e:
            logging.error(f"Failed to remove temporary file: {str(e)}")

    except Exception as e:
        logging.critical(f"Critical backup error: {str(e)}")
        try:
            if archive_path is not None:
                os.remove(archive_path)
                logging.info(f"Temporary file removed: {archive_path}")
        except Exception as e:
            logging.error(f"Failed to remove temporary file: {str(e)}")
        sys.exit(1)


def initial_check():
    try:
        list_nextbox_dir('', '')
        logging.info("Initial check: Authentication and directory access OK")
        if not os.path.isdir(local_dir):
            raise Exception(f'Local directory not found: {local_dir}')
        logging.info("Initial check: Target directory exists")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logging.critical("Initial check: Authentication failed")
        elif e.response.status_code == 404:
            logging.critical("Initial check: NextBox directory not found")
        else:
            logging.critical(f"Initial check: HTTP error ({e.response.status_code})")
        sys.exit(1)

    except Exception as e:
        logging.critical(f"Initial check failed: {str(e)}")
        sys.exit(1)


def main_loop():
    STATE_FILE = "last_backup_time.txt"

    last_backup_time = None
    first_run = 0
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                timestamp_str = f.read().strip()
                last_backup_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        except Exception as e:
            logging.error(f"Failed to read state file: {str(e)}")
            last_backup_time = None
            with open(STATE_FILE, 'w') as f:
                f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
    else:
        first_run = 1

    while True:
        try:
            now = datetime.now()
            base_time = last_backup_time or now
            cron = croniter(cron_schedule, base_time)
            next_run = cron.get_next(datetime)

            if now >= next_run or first_run == 1:
                first_run = 0
                perform_backup()
                last_backup_time = datetime.now()

                with open(STATE_FILE, 'w') as f:
                    f.write(last_backup_time.strftime("%Y-%m-%d %H:%M:%S.%f"))
                continue

            sleep_time = min(300, (next_run - now).total_seconds())
            time.sleep(sleep_time)

        except Exception as e:
            logging.critical(f"Main loop error: {str(e)}")
            sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    try:
        logging.info("Backup service starting...")
        initial_check()
        logging.info("Starting main backup loop")
        main_loop()

    except KeyboardInterrupt:
        logging.info("Backup service stopped by user")
    except Exception as e:
        logging.critical(f"Fatal error: {str(e)}")
        sys.exit(1)