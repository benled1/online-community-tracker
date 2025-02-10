import errno
import logging
import os
import re
import socket
import threading
import time
from datetime import datetime

from pymongo import MongoClient

logger = logging.getLogger(__name__)

class ChatConsumer:
    def __init__(self, channel_name: str) -> None:
        self.channel = channel_name

        self.access_token = os.getenv("TWITCH_ACCESS_TOKEN")
        if not self.access_token:
            raise ValueError("TWITCH_ACCESS_TOKEN not found in environment variables.")

        self.server_host = os.getenv("TWITCH_IRC_SERVER", "irc.chat.twitch.tv")
        self.irc_port = int(os.getenv("TWITCH_IRC_PORT", "6667"))
        self.nick = os.getenv("TWITCH_NICK", "benled_dev")

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(1.0)

        self.message_buffer = []

        mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
        self.client = MongoClient(mongo_uri)
        self.db = self.client.get_database(os.getenv("MONGODB_DB", "chat_db"))
        self.collection = self.db.get_collection(os.getenv("MONGODB_COLLECTION", "chat_messages"))

        self._stop_event = threading.Event()

        self.message_pattern = re.compile(r"^:(\w+)!\w+@\w+\.tmi\.twitch\.tv PRIVMSG #(\w+) :(.+)$")

    def consume_chats(self) -> None:
        try:
            self._connect_to_twitch()
            self._handle_messages()
        finally:
            self._flush_messages()
            self._cleanup()

    def _connect_to_twitch(self) -> None:
        token = f"oauth:{self.access_token}"
        try:
            self.sock.connect((self.server_host, self.irc_port))
            self.sock.sendall(f"PASS {token}\r\n".encode("utf-8"))
            self.sock.sendall(f"NICK {self.nick}\r\n".encode("utf-8"))
            self.sock.sendall(f"JOIN #{self.channel}\r\n".encode("utf-8"))
            logger.info("Connected to Twitch chat in channel: #%s", self.channel)
        except Exception as e:
            logger.exception("Error connecting to Twitch IRC: %s", e)
            raise

    def _handle_messages(self) -> None:
        while not self._stop_event.is_set():
            try:
                response = self.sock.recv(2048).decode("utf-8")
            except socket.timeout:
                continue
            except OSError as e:
                if e.errno == errno.EBADF:
                    logger.info("Socket has been closed. Exiting message loop.")
                    break
                else:
                    logger.error("Error receiving data: %s", e)
                    continue
            except Exception as e:
                logger.error("Error receiving data: %s", e)
                continue

            if not response:
                continue

            if response.startswith("PING"):
                try:
                    self.sock.sendall("PONG :tmi.twitch.tv\r\n".encode("utf-8"))
                    logger.debug("Sent PONG response")
                except Exception as e:
                    logger.error("Error sending PONG: %s", e)
            elif "PRIVMSG" in response:
                start_time = time.time()
                parts = response.split(":", 2)
                if len(parts) > 2:
                    message = parts[2].strip()
                    username = parts[1].split("!")[0]
                    self._insert_message(username, message, self.channel)
                end_time = time.time()
                elapsed_time = end_time - start_time
                logger.debug("Processing time: %.4f seconds", elapsed_time)

    def _process_line(self, line: str) -> None:
        match = self.message_pattern.match(line)
        if match:
            username, channel, message = match.groups()
            self._insert_message(username, message, channel)
        else:
            logger.debug("Unrecognized message format: %s", line)

    def _insert_message(self, sender_name: str, message: str, channel: str) -> None:
        self.message_buffer.append({
            "sender_name": sender_name,
            "message": message,
            "channel": channel,
            "timestamp": datetime.utcnow()
        })
        if len(self.message_buffer) >= 10: self._flush_messages()

    def _flush_messages(self) -> None:
        if self.message_buffer:
            try:
                res = self.collection.insert_many(self.message_buffer)
                logger.info("Inserted %d messages with ids: %s",
                            len(self.message_buffer), res.inserted_ids)
                self.message_buffer.clear()
            except Exception as e:
                logger.exception("Database error while inserting messages: %s", e)

    def stop(self) -> None:
        logger.info("Stopping ChatConsumer for channel: %s", self.channel)
        self._stop_event.set()
        try:
            self.sock.close()
        except Exception as e:
            logger.exception("Error closing socket: %s", e)

    def _cleanup(self) -> None:
        try:
            self.client.close()
        except Exception as e:
            logger.exception("Error closing MongoDB client: %s", e)
