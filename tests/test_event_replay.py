import pytest
from app.services.event_replay import event_replay
from app.db.models.webhook_event import WebhookEvent
from app.db.models.source import Source
import uuid
from app.db.models.event_replay_log import EventReplayLog
from sqlalchemy import select, func


def test_replay_dead_letter_event(db_session):

    source = Source(
        name="replay_test_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="replay_event_001",
        event_type="order.created",
        payload={
            "event_id": "replay_event_001",
            "event_type": "order.created",
            "data": {"order_id": 101, "amount": 250}
        },
        status="dead_letter",
        attempt_count=5,
        max_attempts=5,
        last_error="Previous processing failure"
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    replayed_event = event_replay(db_session, event.id)

    assert replayed_event.status == "retrying"
    assert replayed_event.attempt_count == 0
    assert replayed_event.next_retry_at is None
    assert replayed_event.last_error is None
    assert replayed_event.locked_until is None
    assert replayed_event.processed_at is None


def test_replay_nonexistent_event(db_session):

    with pytest.raises(LookupError):
        event_replay(db_session, uuid.uuid4())


def test_replay_non_dead_letter_event(db_session):

    source = Source(
        name="replay_invalid_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="replay_invalid_001",
        event_type="order.created",
        payload={"data": {"order_id": 101, "amount": 250}},
        status="processed",
        attempt_count=1,
        max_attempts=5
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    with pytest.raises(ValueError):
        event_replay(db_session, event.id)



def test_multiple_replays_create_separate_audit_logs(db_session):

    source = Source(
        name="multiple_replay_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="multiple_replay_001",
        event_type="order.created",
        payload={
            "data": {"order_id": 101, "amount": 250}
        },
        status="dead_letter",
        attempt_count=5,
        max_attempts=5,
        last_error="First processing failure"
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    # First replay
    event_replay(db_session, event.id)

    # Simulate another processing failure
    event.status = "dead_letter"
    event.attempt_count = 3
    event.last_error = "Second processing failure"
    db_session.commit()

    # Second replay
    event_replay(db_session, event.id)

    logs = db_session.query(EventReplayLog).filter(
        EventReplayLog.event_id == event.id
    ).order_by(EventReplayLog.replayed_at).all()

    assert len(logs) == 2

    assert logs[0].previous_status == "dead_letter"
    assert logs[0].previous_attempt_count == 5
    assert logs[0].previous_error == "First processing failure"

    assert logs[1].previous_status == "dead_letter"
    assert logs[1].previous_attempt_count == 3
    assert logs[1].previous_error == "Second processing failure"

    assert event.status == "retrying"
    assert event.attempt_count == 0



def test_replay_transaction_rolls_back_on_commit_failure(db_session,monkeypatch):

    source = Source(
        name="replay_rollback_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="replay_rollback_001",
        event_type="order.created",
        payload={"data": {"order_id": 101, "amount": 250}},
        status="dead_letter",
        attempt_count=5,
        max_attempts=5,
        last_error="Previous processing failure"
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    original_commit = db_session.commit

    def failing_commit():
        raise RuntimeError("Simulated database failure")

    monkeypatch.setattr(db_session, "commit", failing_commit)

    with pytest.raises(RuntimeError):
        event_replay(db_session, event.id)

    # Restore the original commit method
    monkeypatch.setattr(db_session, "commit", original_commit)

    # Verify that the original event state remains unchanged
    db_session.refresh(event)

    assert event.status == "dead_letter"
    assert event.attempt_count == 5
    assert event.last_error == "Previous processing failure"

    # Verify that no audit record was persisted
    log_count = db_session.query(EventReplayLog).filter(
        EventReplayLog.event_id == event.id
    ).count()

    assert log_count == 0
