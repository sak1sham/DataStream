from slack import WebClient
from typing import Dict

def send_str_message(msg: str = "", error_msg: str = "", channel: str = "", slack_token: str = ""):
    client = WebClient(token=slack_token)
    response = client.chat_postMessage(
        channel = channel,
        text = msg
    )
    if(len(error_msg) > 0):
        _ = client.chat_postMessage(
            channel=channel,
            thread_ts=response['ts'],
            text = f"```{error_msg}```"
        )

def send_message(msg: Dict[str, str] = {"message": ""}, channel: str = "", slack_token: str = ""):
    client = WebClient(token=slack_token)
    response = client.chat_postMessage(
        channel = channel,
        text = msg["message"]
    )
    if(msg["error_msg"] and len(msg["error_msg"]) > 0):
        _ = client.chat_postMessage(
            channel=channel,
            thread_ts=response['ts'],
            text = f"```{msg['error_msg']}```"
        )

def send_formatted_message(channel: str = "", slack_token: str = "", **msg):
    total = {'message': ''}
    if(msg['status']):
        total['message'] = f"Migration completed for *{msg['name']}* from database *{msg['database']}* ({msg['db_type']}) to *{msg['destination']}*"
        if('time' in msg.keys()):
            total['message'] += f":\nTotal time taken: {msg['time_taken']}"
    else:
        total['message'] = f"<!channel> Migration stopped for *{msg['name']}* from database *{msg['database']}* ({msg['db_type']}) to *{msg['destination']}*"
        if('reason' in msg.keys()):
            total['message'] += f"\nReason: {msg['reason']} :warning:"
        total['error_msg'] = msg['thread'] if 'thread' in msg.keys() else ""
    if('insertions' in msg.keys() and 'updations' in msg.keys() and msg['insertions'] != -1 and msg['updations'] != -1):
        ins_str = "{:,}".format(msg['insertions'])
        upd_str = "{:,}".format(msg['updations'])
        total['message'] += f"\nInsertions: {ins_str}\nUpdations: {upd_str}"
    send_message(msg=total, channel=channel, slack_token=slack_token)