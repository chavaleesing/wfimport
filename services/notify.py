import requests
import json
import os


def get_line_token() -> str:
    # TODO get token by line api
    return "wfDS0Lz4l6R8StClk2uWajzIN8IRLvmrl2b/5uug6ZdsSg6tx2TL7WfC0dgsvKWft3eoGhtdxKTDYTWPLXWEV3kYvJ8ZRv4FSnw1hsgQL4Mdg+8Eq23pXHI9+BR7goJvJeA8wpq3HHrQqZr6yP7h+o9PbdgDzCFqoOLOYbqAITQ="

def line_alert(text: str) -> None:
    if not int(os.getenv("IS_ALERT", 0)):
        return
    url = "https://api.line.me/v2/bot/message/broadcast"

    payload = json.dumps({
        "messages": [{"type": "text","text": text}]
    })
    token = get_line_token()
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    requests.request("POST", url, headers=headers, data=payload)

def ms_alert(text: str) -> None:
    if not int(os.getenv("IS_ALERT", 0)):
        return
    url = "https://krungthaigroup.webhook.office.com/webhookb2/161bc7af-4955-43d7-ae21-86a8b835c599@b8d867c0-b949-455c-95dd-cee5324ed815/IncomingWebhook/f19316efc73a4c07a494e81914d0499b/bf45723f-aa69-4c27-9222-9f204b590945/V2lurz3wzYWyNBRGqwtmE-1TNbvmg4PQ4VgJo-_z9UUIg1"

    payload = json.dumps({
        "topic": "Importing data from Data inno.",
        "text": text
    })
    headers = {'Content-Type': 'application/json'}
    requests.request("POST", url, headers=headers, data=payload)
