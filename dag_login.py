import requests
from utils import _write_html
import json


def run():
    url = "http://10.106.14.1:12345/dolphinscheduler/login"

    headers = {
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer":"http://10.106.14.1:12345/dolphinscheduler/ui/view/login/index.html"
        }
    data = {"username": "mengy", "password": "mengy_123"}
    res = requests.post(url=url, data=data, headers=headers)

    if res.status_code == 200:
        headers = res.headers
        print(res.headers)
        cookie = headers.get("Set-Cookie")
        cookie = cookie.split(';')[0]
        sessionId = cookie.split('=')[-1]

        data = res.json().get("content",{}).get("data")
        print(data)
        headers = {}
        headers["Cookie"] = cookie + ';language=zh_CN;' + 'sessionId={}'.format(sessionId)
        headers["sessionId"] = sessionId
        headers_json = json.dumps(headers)
        _write_html('dag_headers.hd',headers_json)



if __name__ == '__main__':
    run()