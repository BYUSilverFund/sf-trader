from .broker_client import BrokerClient
from .ibkr_client import ibrk_client
from .test_client import test_client
import datetime as dt


def get_broker(broker_name: str, data_date: dt.date) -> BrokerClient:
    match broker_name:
        case "ibkr":
            return ibrk_client()
        case "test":
            return test_client(data_date)


__all__ = ["BrokerClient", "IBKRClient", "TestClient"]
