import logging
import os
from datetime import datetime

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient

from data_ingestion.channels_monitor import ChannelsMonitor

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

mongo_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
client = MongoClient(mongo_uri)
db_name = os.getenv("MONGODB_DB", "chat_db")
collection_name = os.getenv("MONGODB_COLLECTION", "chat_messages")
db = client[db_name]
collection = db[collection_name]

poll_interval = int(os.getenv("CHANNEL_MONITOR_POLL_INTERVAL", "300"))
monitor = ChannelsMonitor(evaluation_interval=poll_interval, channel_limit=100)

@app.on_event("startup")
def on_startup():
    logger.info("Starting Channel Monitor...")
    monitor.start()

@app.on_event("shutdown")
def on_shutdown():
    logger.info("Stopping Channel Monitor...")
    monitor.stop()
    monitor.close()
    client.close()

@app.get("/messages")
def get_messages(start_time: str, end_time: str):
    """
    Fetch all chat messages within a given UTC date/time range.

    Provide ISO-8601 formatted strings for start_time and end_time (e.g., 2023-01-01T00:00:00).
    """
    try:
        dt_start = datetime.fromisoformat(start_time)
        dt_end = datetime.fromisoformat(end_time)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date/time format. Use ISO-8601.")

    query = {"timestamp": {"$gte": dt_start, "$lte": dt_end}}
    cursor = collection.find(query)
    messages = []
    for doc in cursor:
        doc["_id"] = str(doc["_id"])
        doc["timestamp"] = doc["timestamp"].isoformat()
        messages.append(doc)

    return {"count": len(messages), "messages": messages}

@app.post("/monitor/start")
def start_monitor():
    monitor.start()
    return {"status": "ChannelMonitor started"}

@app.post("/monitor/stop")
def stop_monitor():
    monitor.stop()
    return {"status": "ChannelMonitor stopped"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
