import logging
import os
import threading

import requests
from dotenv import load_dotenv
from pymongo import MongoClient

from data_ingestion.chat_consumer import ChatConsumer

logger = logging.getLogger(__name__)

class ChannelMonitor:
    """
    Monitors Twitch channels and manages chat consumers based on the top channels.
    """
    def __init__(self, poll_interval: int = 300, limit: int = 100):
        """
        :param poll_interval: Polling interval in seconds.
        :param limit: Maximum number of channels to fetch from Twitch.
        """
        self.poll_interval = poll_interval
        self.limit = limit
        self.running = False
        self.channel_consumers: dict[str, tuple[ChatConsumer, threading.Thread]] = {}
        self.thread: threading.Thread | None = None
        self.lock = threading.Lock()
        self.session = requests.Session()
        self._stop_event = threading.Event()

        mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.mongo_client = MongoClient(mongo_uri)

    def _get_top_channels(self) -> set[str]:
        client_id = os.getenv("TWITCH_CLIENT_ID")
        access_token = os.getenv("TWITCH_ACCESS_TOKEN")
        if not client_id or not access_token:
            raise ValueError("TWITCH_CLIENT_ID and TWITCH_ACCESS_TOKEN must be set in the environment.")

        url = "https://api.twitch.tv/helix/streams"
        headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {access_token}",
        }
        params = {"first": self.limit}

        response = self.session.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        streams_data = response.json().get("data", [])
        channels = {
            stream.get("user_login") or stream.get("user_name", "").lower()
            for stream in streams_data
            if stream.get("user_login") or stream.get("user_name")
        }
        return channels

    def _monitor_channels(self) -> None:
        while self.running:
            try:
                top_channels = self._get_top_channels()
                logger.info("Retrieved %d channels from Twitch.", len(top_channels))
            except Exception as e:
                logger.exception("Error retrieving top channels: %s", e)
                if self._stop_event.wait(timeout=10):
                    break
                continue

            with self.lock:
                for channel in top_channels:
                    if channel not in self.channel_consumers:
                        logger.info("Starting chat consumer for channel: %s", channel)
                        consumer = ChatConsumer(channel, self.mongo_client)
                        thread = threading.Thread(
                            target=consumer.consume_chats,
                            daemon=True,
                            name=f"Consumer-{channel}"
                        )
                        self.channel_consumers[channel] = (consumer, thread)
                        thread.start()

                channels_to_remove = [
                    channel for channel in self.channel_consumers if channel not in top_channels
                ]

            for channel in channels_to_remove:
                logger.info("Stopping chat consumer for channel: %s", channel)
                with self.lock:
                    consumer, thread = self.channel_consumers.pop(channel)
                consumer.stop()
                thread.join(timeout=5)

            if self._stop_event.wait(timeout=self.poll_interval):
                break

    def start(self) -> None:
        with self.lock:
            if not self.running:
                logger.info("Starting ChannelMonitor...")
                self.running = True
                self._stop_event.clear()
                self.thread = threading.Thread(
                    target=self._monitor_channels,
                    daemon=True,
                    name="ChannelMonitorThread"
                )
                self.thread.start()

    def stop(self) -> None:
        with self.lock:
            if self.running:
                logger.info("Stopping ChannelMonitor...")
                self.running = False
                self._stop_event.set()
                if self.thread:
                    self.thread.join(timeout=10)
                for channel, (consumer, thread) in list(self.channel_consumers.items()):
                    logger.info("Stopping chat consumer for channel: %s", channel)
                    consumer.stop()
                    thread.join(timeout=5)
                self.channel_consumers.clear()
                logger.info("ChannelMonitor stopped.")

    def close(self) -> None:
        self.session.close()
        self.mongo_client.close()
