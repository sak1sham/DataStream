from slack import WebClient

def send_message(msg: str = "", channel: str = "", slack_token: str = ""):
    client = WebClient(token=slack_token)
    response = client.chat_postMessage(
        channel = channel,
        text = msg
    )