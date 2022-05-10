from slack import WebClient

def send_message(msg: str = "", channel: str = "", slack_token: str = ""):
    client = WebClient(token=slack_token)
    response = client.chat_postMessage(
        channel = channel,
        text = msg
    )

def send_error_message(msg: str = "", error_msg = "", channel: str = "", slack_token: str = ""):
    client = WebClient(token=slack_token)
    response = client.chat_postMessage(
        channel = channel,
        text = msg
    )
    response2 = client.chat_postMessage(
        channel=channel,
        thread_ts=response['ts'],
        text = f"```{error_msg}```"
    )
