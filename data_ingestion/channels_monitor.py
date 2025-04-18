import logging
import os
import threading

import requests
from dotenv import load_dotenv
from pymongo import MongoClient

from data_ingestion.chat_consumer import ChatConsumer

logger = logging.getLogger(__name__)


class ChannelsMonitor:
    """
    Monitors Twitch channels and manages chat consumers based on the top channels.
    """
    def __init__(self, evaluation_interval: int = 300, channel_limit: int = 100):
        """
        :param poll_interval: Polling interval in seconds. Time period between re-evaluating the channels to monitor.
        :param limit: Number of channels to monitor at a given time.
        """
        self.evaluation_interval = evaluation_interval
        self.channel_limit = channel_limit
        self.running = False
        self.thread = None
        self.lock = threading.Lock()
        self.session = requests.Session()
        self._stop_event = threading.Event()

        db_conn_str = os.getenv("DB_CONN_STR", "mongodb://localhost:27017")
        self.mongo_client = MongoClient(db_conn_str) 

    def start(self) -> None:
        """
        Launch a thread for each channel in the top <channel_limit> channels on twitch.
        """
        with self.lock:
            if not self.running:
                logger.info("Starting ChannelMonitor...")
                self._stop_event.clear()
                self.thread = threading.Thread(
                    target=self._monitor_channels,
                    daemon=True,
                    name="ChannelMonitorThread"
                )
                self.thread.start()
                self.running = True
            else:
                raise RuntimeError("Channel monitor already running.")

    def stop(self) -> None:
        with self.lock:
            if self.running:
                logger.info("Stopping ChannelMonitor...")
                self._stop_event.set()
                if self.thread:
                    self.thread.join(timeout=10)
                    self.thread = None
                self.running = False
                logger.info("ChannelMonitor stopped.")
            else:
               raise RuntimeError("Channel monitor not running.")

    def close(self) -> None:
        self.session.close()
        self.mongo_client.close()

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
        params = {"first": self.channel_limit}

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

            # retrieve channels to monitor
            try:
                top_channels = self._get_top_channels()
                logger.info("Retrieved %d channels from Twitch.", len(top_channels))
            except Exception as e:
                logger.exception("Error retrieving top channels: %s", e)
                if self._stop_event.wait(timeout=10):
                    break
                continue

            # update channels being monitored
            channel_consumers: dict[str, tuple[ChatConsumer, threading.Thread]] = {}
            with self.lock:
                for channel in top_channels:
                    if channel not in channel_consumers:
                        logger.info("Starting chat consumer for channel: %s", channel)
                        consumer = ChatConsumer(channel, self.mongo_client)
                        thread = threading.Thread(
                            target=consumer.consume_chats,
                            daemon=True,
                            name=f"Consumer-{channel}"
                        )
                        channel_consumers[channel] = (consumer, thread)
                        thread.start()

                channels_to_remove = [
                    channel for channel in channel_consumers if channel not in top_channels
                ]

                for channel in channels_to_remove:
                    logger.info("Stopping chat consumer for channel: %s", channel)
                    consumer, thread = channel_consumers.pop(channel)
                    consumer.stop()
                    thread.join(timeout=5)

            if self._stop_event.wait(timeout=self.evaluation_interval):
                for channel, (consumer, thread) in list(channel_consumers.items()):
                    logger.info("Stopping chat consumer for channel: %s", channel)
                    consumer.stop()
                    thread.join(timeout=5)
                channel_consumers.clear()

