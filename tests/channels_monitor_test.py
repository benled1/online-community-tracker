import pytest
from unittest.mock import patch, MagicMock
from data_ingestion import ChannelsMonitor
from tests.custom_mocks import DummyThread


@patch.object(ChannelsMonitor, "_monitor_channels", lambda self: None)
@patch("data_ingestion.channels_monitor.MongoClient", new=MagicMock)
@patch("data_ingestion.channels_monitor.threading.Thread", new=DummyThread)
def test_start():
    monitor = ChannelsMonitor(evaluation_interval=1, channel_limit=1)
    assert monitor.running == False
    assert monitor.thread == None
    monitor.start()
    assert monitor.running == True
    assert isinstance(monitor.thread, DummyThread)
    with pytest.raises(RuntimeError):
        monitor.start()

@patch.object(ChannelsMonitor, "_monitor_channels", lambda self: None)
@patch("data_ingestion.channels_monitor.MongoClient", new=MagicMock)
@patch("data_ingestion.channels_monitor.threading.Thread", new=DummyThread)
def test_stop_cleans_up_and_stops_thread():
    monitor = ChannelsMonitor(evaluation_interval=1, channel_limit=1)
    monitor.start()
    assert monitor.running == True
    assert isinstance(monitor.thread, DummyThread)
    monitor.stop()
    assert monitor.running == False
    assert monitor.thread == None
    with pytest.raises(RuntimeError):
        monitor.stop()
    
