
import uuid
import pytest

from app.db.models import Source, WebhookEvent


def create_dead_letter_event(db_session):

    source = Source(
        name="replay_api_test_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="replay_api_001",
        event_type="order.created",
        payload={
            "event_id": "replay_api_001",
            "event_type": "order.created",
            "data": {
                "order_id": 101,
                "amount": 250
            }
        },
        status="dead_letter",
        attempt_count=5,
        max_attempts=5,
        last_error="Previous processing failure"
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    return event


def test_replay_dead_letter_event_api(client, db_session):

    event = create_dead_letter_event(db_session)

    response = client.post(
        f"/webhooks/events/{event.id}/replay"
    )

    assert response.status_code == 200
    assert response.json()["status"] == "retrying"
    assert response.json()["event_id"] == str(event.id)
    assert response.json()["message"] == "Event scheduled for replay"

    db_session.refresh(event)

    assert event.status == "retrying"
    assert event.attempt_count == 0
    assert event.last_error is None


def test_replay_nonexistent_event_api(client):

    unknown_event_id = uuid.uuid4()

    response = client.post(
        f"/webhooks/events/{unknown_event_id}/replay"
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Webhook event not found"


def test_replay_non_dead_letter_event_api(client, db_session):

    event = create_dead_letter_event(db_session)

    event.status = "processed"
    db_session.commit()

    response = client.post(
        f"/webhooks/events/{event.id}/replay"
    )

    assert response.status_code == 409
    assert response.json()["detail"] == (
        "Only dead-letter events can be replayed"
    )


def test_replay_invalid_event_id_api(client):

    response = client.post(
        "/webhooks/events/not-a-valid-uuid/replay"
    )

    assert response.status_code == 422
