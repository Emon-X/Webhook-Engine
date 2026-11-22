import pytest
from types import SimpleNamespace
from app.core.exceptions import PermanentProcessingError, TransientProcessingError
from app.handlers.orders import handle_order_created


def test_handle_valid_order_created():

    event = SimpleNamespace(
        payload={
            "event_id": "evt_001",
            "event_type": "order.created",
            "data": {
                "order_id": 101,
                "amount": 250
            }
        }
    )

    # Should execute without raising an exception.
    handle_order_created(event)


def test_handle_order_created_missing_data():

    event = SimpleNamespace(
        payload={
            "event_id": "evt_002",
            "event_type": "order.created",
            "data": {
                "order_id": 101
            }
        }
    )

    with pytest.raises(PermanentProcessingError):
        handle_order_created(event)


def test_handle_order_created_invalid_data():

    event = SimpleNamespace(
        payload={
            "event_id": "evt_003",
            "event_type": "order.created",
            "data": None
        }
    )

    with pytest.raises(PermanentProcessingError):
        handle_order_created(event)