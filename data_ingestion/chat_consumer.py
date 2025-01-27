import os
import socket

import requests
from dotenv import load_dotenv


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
        """
        Validates the given access token using Twitch's /validate endpoint.
        Returns a dict containing validation info if token is valid, or raises an exception if invalid.
        """
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
        """Receive and print chat messages."""
        try:
            while True:
                response = self.sock.recv(2048).decode("utf-8")
                
                if response.startswith("PING"):
                    self.sock.send("PONG :tmi.twitch.tv\n".encode("utf-8"))
                
                elif "PRIVMSG" in response:
                    parts = response.split(":", 2)
                    if len(parts) > 2:
                        message = parts[2].strip()
                        username = parts[1].split("!")[0]
                        print(f"From{self.channel}: {username}: {message}")
        
        except KeyboardInterrupt:
            print("\nDisconnected from Twitch chat.")
        except Exception as e:
            print(f"Error: {e}")
