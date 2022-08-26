import requests
from utils import _write_html
import json


def set_cookie():
    url = "http://10.106.14.1:1521/index.html"
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"}
    res = requests.get(url,headers=headers)
    if res.status_code == 200:
        headers = res.headers
        setcookie = headers.get("Set-Cookie")
        setcookie_list = ";".join(setcookie.split(",")).split(";")
        key_cookie_list = [cookie.strip(" ") for cookie in setcookie_list if cookie.strip(' ').split("=")[0] in key_list]
        cookie = ";".join(key_cookie_list)
        return cookie

def run():
    url = "http://10.106.14.1:1521/api/auth/login"

    headers = {
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36",
        "Content-Type": "application/json"
        }
    data = {"username": "bi_bdp", "password": "bi_bdp", "rememberMe": 1}
    res = requests.post(url=url, json=data, headers=headers)

    if res.status_code == 200:
        headers = res.headers
        token = headers.get("token")
        if res.json().get("code") == 200:
            data = res.json().get("content",{}).get("data")
            print(data)
            headers = {}
            headers["Cookie"] = "Admin-Token=" + data
            headers["Authorization"] = data
            headers_json = json.dumps(headers)

            _write_html('source_headers.hd',headers_json)



if __name__ == '__main__':
    run()