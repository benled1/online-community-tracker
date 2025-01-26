import os
import socket

import requests
from dotenv import load_dotenv

load_dotenv()
ACCESS_TOKEN = os.getenv("TWITCH_ACCESS_TOKEN")
REFRESH_TOKEN = os.getenv("TWITCH_REFRESH_TOKEN")
HOST = "irc.chat.twitch.tv"
PORT = 6667
NICK = "benled_dev"
CHANNEL = "loltyler1"

VALIDATE_URL = "https://id.twitch.tv/oauth2/validate"


def validate_access_token(token: str) -> dict:
    """
    Validates the given access token using Twitch's /validate endpoint.
    Returns a dict containing validation info if token is valid, or raises an exception if invalid.
    """
    headers = {
        "Authorization": f"OAuth {token}"
    }
    response = requests.get(VALIDATE_URL, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError(f"Token validation failed: {response.status_code} {response.text}")

def connect_to_twitch(bearer_token: str):
    """Connect to Twitch IRC server and authenticate."""
    token = f"oauth:{ACCESS_TOKEN}"
    sock = socket.socket()
    sock.connect((HOST, PORT))
    sock.send(f"PASS {token}\n".encode("utf-8"))  
    sock.send(f"NICK {NICK}\n".encode("utf-8"))
    sock.send(f"JOIN #{CHANNEL}\n".encode("utf-8"))
    print(f"Connected to Twitch chat in #{CHANNEL}")
    return sock

def handle_messages(sock):
    """Receive and print chat messages."""
    try:
        while True:
            response = sock.recv(2048).decode("utf-8")
            
            if response.startswith("PING"):
                sock.send("PONG :tmi.twitch.tv\n".encode("utf-8"))
            
            elif "PRIVMSG" in response:
                parts = response.split(":", 2)
                if len(parts) > 2:
                    message = parts[2].strip()
                    username = parts[1].split("!")[0]
                    print(f"{username}: {message}")
    
    except KeyboardInterrupt:
        print("\nDisconnected from Twitch chat.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    validate_access_token(ACCESS_TOKEN)
    sock = connect_to_twitch(bearer_token=None)
    handle_messages(sock)
