import os
import socket
import time

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2 import sql


class ChatConsumer:
    def __init__(self, channel_name: str):
        self.access_token = os.getenv("TWITCH_ACCESS_TOKEN")
        self.refresh_token = os.getenv("TWITCH_REFRESH_TOKEN")
        self.token_validation_url = "https://id.twitch.tv/oauth2/validate"
        self.server_host = "irc.chat.twitch.tv"
        self.irc_port = 6667
        self.channel = channel_name
        self.nick = "benled_dev"
        self.sock = socket.socket()

    def consume_chats(self):
        self._validate_access_token()
        self._connect_to_twitch()
        self._handle_messages()

    def _validate_access_token(self) -> dict:
        """Validates the given access token using Twitch's /validate endpoint."""
        headers = {
            "Authorization": f"OAuth {self.access_token}"
        }
        response = requests.get(self.token_validation_url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f"Token validation failed: {response.status_code} {response.text}")

    def _connect_to_twitch(self):
        """Connect to Twitch IRC server and authenticate."""
        token = f"oauth:{self.access_token}"
        self.sock.connect((self.server_host, self.irc_port))
        self.sock.send(f"PASS {token}\n".encode("utf-8"))  
        self.sock.send(f"NICK {self.nick}\n".encode("utf-8"))
        self.sock.send(f"JOIN #{self.channel}\n".encode("utf-8"))
        print(f"Connected to Twitch chat in #{self.channel}")

    def _handle_messages(self):
        """Handle chat messages and insert into database."""
        try:
            while True:
                response = self.sock.recv(2048).decode("utf-8")

                if response.startswith("PING"):
                    self.sock.send("PONG :tmi.twitch.tv\n".encode("utf-8"))

                elif "PRIVMSG" in response:
                    start_time = time.time()

                    parts = response.split(":", 2)
                    if len(parts) > 2:
                        message = parts[2].strip()
                        username = parts[1].split("!")[0]
                        print(f"{username}: {message}")

                        self._insert_message(username, message, self.channel)

                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    print(f"Processing time: {elapsed_time:.4f} seconds")

        except KeyboardInterrupt:
            print("\nDisconnected from Twitch chat.")
        except Exception as e:
            print(f"Error: {e}")

    def _insert_message(self, sender_name, message, channel):
        """Insert a message into the chat_messages table."""
        conn = None
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="password",
                host="localhost",
                port="5432"
            )
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id SERIAL PRIMARY KEY,
                    sender_name TEXT NOT NULL,
                    message TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    timestamp TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            conn.commit()

            cur.execute(
                "INSERT INTO chat_messages (sender_name, message, channel) VALUES (%s, %s, %s);",
                (sender_name, message, channel)
            )
            
            conn.commit()
            cur.close()
            print(f"Message from {sender_name} in #{channel} inserted successfully.")

        except Exception as e:
            print("Database Error:", e)
        finally:
            if conn:
                conn.close()
