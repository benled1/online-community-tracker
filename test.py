import os
from dotenv import load_dotenv
from data_ingestion import ChannelsMonitor

load_dotenv()

poll_interval = int(os.getenv("CHANNEL_MONITOR_POLL_INTERVAL", "300"))
monitor = ChannelsMonitor(evaluation_interval=poll_interval, channel_limit=100)
print(monitor._get_top_channels())
