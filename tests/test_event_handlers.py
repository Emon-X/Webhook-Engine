
import threading

import pytest
from types import SimpleNamespace

from app.services.event_handlers import register_handler,dispatch_event


def test_register_and_dispatch_handler():
    
    event_type = "test.registry.success"
    received_events = []

    cancellation_event = threading.Event()

    def test_handler(event, cancellation_event):
        received_events.append((event, cancellation_event))

    register_handler(event_type, test_handler)

    event = SimpleNamespace(
        event_type=event_type,
        payload={"key": "value"}
    )

    dispatch_event(event, cancellation_event)

    assert received_events == [(event, cancellation_event)]


def test_dispatch_unregistered_event():
    event = SimpleNamespace(
        event_type="test.registry.unknown",
        payload={}
    )
    cancellation_event = threading.Event()

    with pytest.raises(LookupError):
        dispatch_event(event, cancellation_event)


def test_prevent_duplicate_handler_registration():
    event_type = "test.registry.duplicate"

    def first_handler(event,cancellation_event):
        pass

    def second_handler(event,cancellation_event):
        pass

    register_handler(event_type, first_handler)

    with pytest.raises(ValueError):
        register_handler(event_type, second_handler)
