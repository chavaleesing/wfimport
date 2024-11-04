import requests
import json
import os


def line_alert(text: str):
    if not int(os.getenv("IS_ALERT", 0)):
        return
    url = "https://api.line.me/v2/bot/message/broadcast"

    payload = json.dumps({
        "messages": [{"type": "text","text": text}]
    })
    headers = {
        'Authorization': 'Bearer wfDS0Lz4l6R8StClk2uWajzIN8IRLvmrl2b/5uug6ZdsSg6tx2TL7WfC0dgsvKWft3eoGhtdxKTDYTWPLXWEV3kYvJ8ZRv4FSnw1hsgQL4Mdg+8Eq23pXHI9+BR7goJvJeA8wpq3HHrQqZr6yP7h+o9PbdgDzCFqoOLOYbqAITQ=',
        'Content-Type': 'application/json'
    }
    requests.request("POST", url, headers=headers, data=payload)
