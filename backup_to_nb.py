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
import uuid
import mimetypes
import glob
import signal


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
    response = requests.delete(nextbox_host + "/storage/element", headers=headers, params=params, timeout=30)
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
            json=payload,
            timeout=30
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
        response = requests.get(nextbox_host + "/storage", headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()["rows"]

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            # Попробуем создать директорию
            if create_directory(nextbox_dir, divide_id):
                # Повторяем запрос после создания директории
                response = requests.get(nextbox_host + "/storage", headers=headers, params=params, timeout=30)
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
    file_size = os.path.getsize(file_path)
    
    # Проверяем доступную память
    try:
        import psutil
        available_memory = psutil.virtual_memory().available
        logging.info(f"Available memory: {available_memory / (1024*1024):.1f} MB")
        
        if file_size > available_memory * 0.3:  # Если файл больше 30% доступной памяти
            logging.warning(f"File size ({file_size / (1024*1024):.1f} MB) is large relative to available memory")
    except ImportError:
        logging.info("psutil not available, cannot check memory")
    
    headers = request_header()
    
    logging.info(f"Starting upload of {file_name} ({file_size / (1024*1024):.1f} MB) to {target_dir}")
    
    try:
        # Используем потоковую загрузку с небольшими чанками
        chunk_size = 1024 * 1024  # 1MB чанки для экономии памяти
        
        # Формируем boundary вручную для потоковой отправки
        boundary = str(uuid.uuid4())
        headers['Content-Type'] = f'multipart/form-data; boundary={boundary}'
        
        # Увеличиваем timeout для больших файлов
        upload_timeout = 600 + (file_size // (1024 * 1024)) * 2
        logging.info(f"Upload timeout set to {upload_timeout} seconds")
        
        # Создаем итератор данных для потоковой отправки
        def generate_data():
            # Начало multipart
            yield f'--{boundary}\r\n'.encode()
            yield f'Content-Disposition: form-data; name="path"\r\n\r\n'.encode()
            yield f'{target_dir}\r\n'.encode()
            
            if divide_id != 0:
                yield f'--{boundary}\r\n'.encode()
                yield f'Content-Disposition: form-data; name="divide_id"\r\n\r\n'.encode()
                yield f'{int(divide_id)}\r\n'.encode()
            
            # Файл
            yield f'--{boundary}\r\n'.encode()
            yield f'Content-Disposition: form-data; name="file"; filename="{file_name}"\r\n'.encode()
            yield f'Content-Type: application/octet-stream\r\n\r\n'.encode()
            
            # Читаем файл по частям
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk
            
            # Конец multipart
            yield f'\r\n--{boundary}--\r\n'.encode()
        
        logging.info("Starting streaming upload...")
        start_time = time.time()
        
        response = requests.post(
            nextbox_host + "/storage/files",
            headers=headers,
            data=generate_data(),
            timeout=upload_timeout,
            stream=True
        )
        
        upload_duration = time.time() - start_time
        logging.info(f"Upload completed in {upload_duration:.2f} seconds")
        
        response.raise_for_status()
        logging.info(f"Successfully uploaded {file_name}")
        
    except MemoryError as e:
        logging.error(f"Memory error during upload: {str(e)}")
        logging.error("Try reducing the file size or increasing system memory")
        raise
    except Exception as e:
        logging.error(f"Failed to upload {file_name}: {str(e)}")
        raise


def create_backup_archive():
    dir_name = os.path.basename(os.path.normpath(local_dir))
    timestamp = datetime.now().strftime("%d.%m.%Y_%H_%M_%S")
    archive_name = f"{dir_name}_{timestamp}.zip"
    archive_path = os.path.join(tempfile.gettempdir(), archive_name)

    logging.info(f"Creating backup archive: {archive_name}")
    start_time = time.time()
    file_count = 0
    
    try:
        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(local_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, local_dir)
                    zipf.write(file_path, arcname)
                    file_count += 1
                    
                    # Логируем прогресс каждые 1000 файлов
                    if file_count % 1000 == 0:
                        logging.info(f"Archived {file_count} files...")

        duration = time.time() - start_time
        archive_size = os.path.getsize(archive_path)
        logging.info(f"Archive created successfully: {file_count} files, {archive_size} bytes, took {duration:.2f} seconds")
        
        return archive_path
        
    except Exception as e:
        logging.error(f"Failed to create archive: {str(e)}")
        # Удаляем неполный архив при ошибке
        if os.path.exists(archive_path):
            try:
                os.remove(archive_path)
            except:
                pass
        raise


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


def cleanup_temp_files():
    """Очищает старые временные файлы бекапов"""
    try:
        dir_name = os.path.basename(os.path.normpath(local_dir))
        temp_pattern = os.path.join(tempfile.gettempdir(), f"{dir_name}_*.zip")
        temp_files = glob.glob(temp_pattern)
        
        if temp_files:
            logging.info(f"Found {len(temp_files)} temporary backup files to cleanup")
            for temp_file in temp_files:
                try:
                    os.remove(temp_file)
                    logging.info(f"Removed temporary file: {temp_file}")
                except Exception as e:
                    logging.warning(f"Failed to remove temporary file {temp_file}: {str(e)}")
        else:
            logging.info("No temporary backup files found")
    except Exception as e:
        logging.warning(f"Cleanup of temporary files failed: {str(e)}")


def initial_check():
    try:
        # Очищаем старые временные файлы при запуске
        cleanup_temp_files()
        
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
                logging.info(f"Backup scheduled to run at {next_run}, current time: {now}")
                perform_backup()
                last_backup_time = datetime.now()

                with open(STATE_FILE, 'w') as f:
                    f.write(last_backup_time.strftime("%Y-%m-%d %H:%M:%S.%f"))
                continue

            sleep_time = min(300, (next_run - now).total_seconds())
            logging.debug(f"Next backup scheduled for {next_run}, sleeping for {sleep_time} seconds")
            time.sleep(sleep_time)

        except Exception as e:
            logging.critical(f"Main loop error: {str(e)}")
            sys.exit(1)


def signal_handler(signum, frame):
    """Обработчик сигналов для корректного завершения"""
    logging.info(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)


if __name__ == "__main__":
    # Устанавливаем обработчики сигналов
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
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