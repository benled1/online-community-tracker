import os
import socket
import threading

import requests
from dotenv import load_dotenv

from data_ingestion import ChatConsumer

load_dotenv()

if __name__ == "__main__":
    caseoh_consumer = ChatConsumer("caseoh_")
    stableronaldo_consumer = ChatConsumer("stableronaldo")

    # either have to create asnchronous consumer or execute each in separate thread.
    stableronaldo_consumer_thread = threading.Thread(target=stableronaldo_consumer.consume_chats)
    caseoh_consumer_thread = threading.Thread(target=caseoh_consumer.consume_chats)
    stableronaldo_consumer_thread.start()
    caseoh_consumer_thread.start()

    stableronaldo_consumer_thread.join()
    caseoh_consumer_thread.join()

