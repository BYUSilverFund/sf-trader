from .broker_client import BrokerClient
from .ibkr_client import IBKRClient
from .test_client import TestClient
import datetime as dt


def get_broker(broker_name: str, data_date: dt.date) -> BrokerClient:
    match broker_name:
        case "ibkr":
            return IBKRClient()
        case "test":
            return TestClient(data_date)


__all__ = ["BrokerClient", "IBKRClient", "TestClient"]
