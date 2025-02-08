import os
import threading
import requests
from dotenv import load_dotenv

from data_ingestion import ChatConsumer

load_dotenv()

def get_top_channels(limit=100):
    client_id = os.getenv("TWITCH_CLIENT_ID")
    access_token = os.getenv("TWITCH_ACCESS_TOKEN")
    if not client_id or not access_token:
        raise ValueError("TWITCH_CLIENT_ID and TWITCH_ACCESS_TOKEN must be set in the environment.")

    url = "https://api.twitch.tv/helix/streams"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {access_token}",
    }
    params = {
        "first": limit,
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    streams_data = response.json().get("data", [])

    channels = []
    for stream in streams_data:
        channel = stream.get("user_login")
        if not channel:
            channel = stream.get("user_name", "").lower()
        channels.append(channel)
    return channels

if __name__ == "__main__":
    try:
        target_channels = get_top_channels(limit=100)
        print(f"Retrieved {len(target_channels)} channels from Twitch.")
    except Exception as e:
        print(f"Error retrieving top channels: {e}")
        exit(1)

    channel_threads = []
    for channel in target_channels:
        print(f"Starting chat consumer for channel: {channel}")
        channel_consumer = ChatConsumer(channel)
        thread = threading.Thread(target=channel_consumer.consume_chats)
        thread.daemon = True
        channel_threads.append(thread)
        thread.start()

    for thread in channel_threads:
        thread.join()
