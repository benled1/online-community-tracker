import os
import socket
import time
from datetime import datetime
import threading

import requests
from dotenv import load_dotenv
from pymongo import MongoClient


class ChatConsumer:
    def __init__(self, channel_name: str) -> None:
        load_dotenv()

        self.access_token = os.getenv("TWITCH_ACCESS_TOKEN")
        self.refresh_token = os.getenv("TWITCH_REFRESH_TOKEN")
        self.token_validation_url = "https://id.twitch.tv/oauth2/validate"
        self.server_host = "irc.chat.twitch.tv"
        self.irc_port = 6667
        self.channel = channel_name
        self.nick = "benled_dev"
        self.sock = socket.socket()
        self.message_buffer = []

        self.client = MongoClient("mongodb://localhost:27017")
        self.db = self.client["chat_db"]
        self.collection = self.db["chat_messages"]

        # Create an event to allow graceful shutdown of the chat consumer
        self._stop_event = threading.Event()
    
    def __del__(self) -> None:
        self.client.close()

    def consume_chats(self) -> None:
        self._connect_to_twitch()
        self._handle_messages()

    def _connect_to_twitch(self) -> None:
        token = f"oauth:{self.access_token}"
        try:
            self.sock.connect((self.server_host, self.irc_port))
            # Set a timeout so recv doesn't block forever,
            # allowing periodic checks for the stop event.
            self.sock.settimeout(1.0)
            self.sock.send(f"PASS {token}\n".encode("utf-8"))
            self.sock.send(f"NICK {self.nick}\n".encode("utf-8"))
            self.sock.send(f"JOIN #{self.channel}\n".encode("utf-8"))
            print(f"Connected to Twitch chat in #{self.channel}")
        except socket.timeout as e:
            print(f"Connection to Twitch IRC timed out: {e}")
        except Exception as e:
            print(f"Error while connecting to Twitch IRC: {e}")

    def _handle_messages(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    response = self.sock.recv(2048).decode("utf-8")
                except socket.timeout:
                    # Timeout occurred; check the stop event and continue looping.
                    continue
                except Exception as e:
                    print(f"Error receiving data: {e}")
                    continue

                if not response:
                    # If the response is empty, the socket may be closed.
                    continue

                if response.startswith("PING"):
                    try:
                        self.sock.send("PONG :tmi.twitch.tv\n".encode("utf-8"))
                    except Exception as e:
                        print(f"Error sending PONG: {e}")
                elif "PRIVMSG" in response:
                    start_time = time.time()

                    parts = response.split(":", 2)
                    if len(parts) > 2:
                        message = parts[2].strip()
                        username = parts[1].split("!")[0]
                        # print(f"{username}: {message}")

                        self._insert_message(username, message, self.channel)

                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    # print(f"Processing time: {elapsed_time:.4f} seconds")

        except KeyboardInterrupt:
            print("\nDisconnected from Twitch chat.")
        except Exception as e:
            print(f"Error in message handling: {e}")

    def _insert_message(self, sender_name, message, channel) -> None:
        try:
            self.message_buffer.append({
                "sender_name": sender_name,
                "message": message,
                "channel": channel,
                "timestamp": datetime.utcnow()
            })
            if len(self.message_buffer) >= 10:
                res = self.collection.insert_many(self.message_buffer)
                print(f"Successfully inserted {len(self.message_buffer)} messages with ids = {res.inserted_ids}")
                self.message_buffer.clear()
        except Exception as e:
            print("Database Error:", e)

    def stop(self) -> None:
        """Signal the consumer to stop and close the connection."""
        self._stop_event.set()
        try:
            self.sock.close()
        except Exception as e:
            print(f"Error closing socket: {e}")
