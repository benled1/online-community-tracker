import os
import socket
import threading

import requests
from dotenv import load_dotenv

from data_ingestion import ChatConsumer

load_dotenv()

if __name__ == "__main__":
    jynxi_consumer = ChatConsumer("jynxzi")
    seagull_consumer = ChatConsumer("a_seagull")
    shroud_consumer = ChatConsumer("shroud")

    jynxi_consumer_thread = threading.Thread(target=jynxi_consumer.consume_chats)
    seagull_consumer_thread = threading.Thread(target=seagull_consumer.consume_chats)
    shroud_consumer_thread = threading.Thread(target=shroud_consumer.consume_chats)

    jynxi_consumer_thread.start()
    seagull_consumer_thread.start()
    shroud_consumer_thread.start()

    jynxi_consumer_thread.join()
    seagull_consumer_thread.join()
    shroud_consumer_thread.join()

