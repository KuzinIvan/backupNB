import requests
import os
import argparse
import base64

parser = argparse.ArgumentParser(description='Simple parser custom argument')

parser.add_argument('--path', type=str, default="Windows")
args = parser.parse_args()

host = "https://nb.bonus-bot.ru/api/v1"
dir_file = "compile"

auth_string = base64.b64encode("admin:tQQKUEj4Ri35dfdktn#ggd4334!fdmn".encode("UTF-8")).decode("UTF-8")


def request_header():
    req_headers = {
        "Content-Type": "application/json",
        "Accept-Language": "ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7",
        "X-PERFORMANCE-TOKEN": "WCNjc1QJU5DIylWOUA3T1J6JkZBe2xSQlomY2NT",
        "Authorization": f"Basic {str(auth_string)}"
    }
    return req_headers


def delete_all(path):
    param_get = {"path": path, "offset": 0, "limit": 100}
    headers = request_header()
    response = requests.get(host + "/storage", headers=headers, params=param_get)
    assert response.status_code < 300, f'{response.text}'
    list_old_file = response.json()["rows"]
    for i in list_old_file:
        param_delete = {"path": i["full_path"]}
        requests.delete(host + "/storage/element", headers=headers, params=param_delete)


def load_file(nameFile, pathBody):
    files = {'file': (nameFile, open(os.path.join(dir_file, nameFile), 'rb'))}
    values = {'path': pathBody}
    headers = request_header()
    headers.pop("Content-Type")
    response = requests.post(host + "/storage/files", headers=headers, files=files, data=values)
    assert response.status_code < 300, f'{response.text}'


delete_all(f"/{args.path}")
for f in os.listdir(dir_file):
    load_file(f, f"/{args.path}")
