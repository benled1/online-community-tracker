import os
import threading
import requests
import time
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
        # Use user_login if available; otherwise, use user_name in lowercase
        channel = stream.get("user_login") or stream.get("user_name", "").lower()
        channels.append(channel)
    return channels

if __name__ == "__main__":
    poll_interval = 60  # seconds between polls
    # A dictionary to map channel name -> (ChatConsumer instance, thread)
    channel_consumers = {}

    while True:
        try:
            top_channels = set(get_top_channels(limit=100))
            print(f"Retrieved {len(top_channels)} channels from Twitch.")
        except Exception as e:
            print(f"Error retrieving top channels: {e}")
            time.sleep(poll_interval)
            continue

        # Start chat consumers for new channels that are in the top channels
        for channel in top_channels:
            if channel not in channel_consumers:
                print(f"Starting chat consumer for channel: {channel}")
                consumer = ChatConsumer(channel)
                thread = threading.Thread(target=consumer.consume_chats)
                thread.daemon = True
                channel_consumers[channel] = (consumer, thread)
                thread.start()

        # Stop and remove consumers for channels that are no longer in the top channels
        channels_to_remove = [channel for channel in channel_consumers if channel not in top_channels]
        for channel in channels_to_remove:
            print(f"Stopping chat consumer for channel: {channel}")
            consumer, thread = channel_consumers[channel]
            consumer.stop()  # Signal the consumer to shut down gracefully
            thread.join()    # Wait for the thread to finish
            del channel_consumers[channel]

        # Wait for the next polling interval
        time.sleep(poll_interval)
