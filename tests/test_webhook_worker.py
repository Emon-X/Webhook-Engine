from app.services.webhook_worker import(
    mark_event_as_failed,
    mark_event_as_processed,
    claim_next_event,
    process_next_event
)
from app.db.models.webhook_event import WebhookEvent
from app.db.models.source import Source
from app.services.event_handlers import dispatch_event
from app.handlers.register import register_handlers
from datetime import datetime,timezone


def test_claim_next_event(db_session):

    source = Source(
        name="test_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    webhook_event = WebhookEvent(
        source_id=source.id,
        external_event_id="event_1",
        event_type="test_event",
        payload={"key": "value"},
        status="received",
        attempt_count=0,
        max_attempts=5,
        next_retry_at=None
    )

    db_session.add(webhook_event)
    db_session.commit()

    event = db_session.query(WebhookEvent).first()

    assert event is not None
    assert event.status == "received"

    claimed_event = claim_next_event(db_session)

    assert claimed_event is not None
    assert claimed_event.status == "processing"
    assert claimed_event.attempt_count == 1



def test_mark_event_processed(db_session):
    
    source = Source(
        name="processing_test_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="event_processed_1",
        event_type="test_event",
        payload={"key": "value"},
        status="processing",
        attempt_count=1,
        max_attempts=5
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    processed_event = mark_event_as_processed(
        db_session,
        event,
        expected_claim_version=event.claim_version
    )

    assert processed_event.status == "processed"
    assert processed_event.processed_at is not None
    assert processed_event.next_retry_at is None
    assert processed_event.last_error is None


def test_mark_event_failed(db_session):
    
    source = Source(
        name="failed_test_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="event_failed_1",
        event_type="test_event",
        payload={"key": "value"},
        status="processing",
        attempt_count=1,
        max_attempts=5
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    failed_event = mark_event_as_failed(
        db_session,
        event,
        "External service unavailable",
        expected_claim_version=event.claim_version
    )
    
    assert failed_event.status == "retrying"
    assert failed_event.last_error == "External service unavailable"
    assert failed_event.next_retry_at is not None
    assert failed_event.attempt_count == 1
    
def test_retry_uses_exponential_backoff(db_session):
    source = Source(
        name="exponential_backoff_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.flush()

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="backoff_event_1",
        event_type="test_event",
        payload={"key": "value"},
        status="processing",
        attempt_count=1,
        max_attempts=10
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    first_retry = mark_event_as_failed(
        db_session,
        event,
        "Temporary failure",
        expected_claim_version=event.claim_version
    )

    first_delay = (
        first_retry.next_retry_at - datetime.now(timezone.utc)
    ).total_seconds()

    assert 1 <= first_delay <= 2

    # Simulate another processing attempt.
    first_retry.status = "processing"
    first_retry.attempt_count = 2
    db_session.commit()

    second_retry = mark_event_as_failed(
        db_session,
        first_retry,
        "Temporary failure",
        expected_claim_version=first_retry.claim_version
    )

    second_delay = (
        second_retry.next_retry_at - datetime.now(timezone.utc)
    ).total_seconds()

    assert 3 <= second_delay <= 4
    assert second_delay > first_delay
    
def test_retry_delay_never_exceeds_one_hour(db_session):

    source = Source(
        name="max_backoff_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.flush()

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="max_backoff_event",
        event_type="test_event",
        payload={"key": "value"},
        status="processing",
        attempt_count=15,
        max_attempts=20
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    failed_event = mark_event_as_failed(
        db_session,
        event,
        "Temporary failure",
        expected_claim_version=event.claim_version
    )

    delay = (
        failed_event.next_retry_at - datetime.now(timezone.utc)
    ).total_seconds()

    assert failed_event.status == "retrying"
    assert 3598 <= delay <= 3600
    
def test_mark_event_failed_dead_letter(db_session):
    
    source = Source(
        name="dead_letter_test_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="event_dead_letter_1",
        event_type="test_event",
        payload={"key": "value"},
        status="processing",
        attempt_count=5,
        max_attempts=5
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    failed_event = mark_event_as_failed(
        db_session,
        event,
        "Maximum retry attempts reached",
        expected_claim_version=event.claim_version
    )
    
    assert failed_event.status == "dead_letter"
    assert failed_event.last_error == "Maximum retry attempts reached"
    assert failed_event.next_retry_at is None
    assert failed_event.attempt_count == 5


def test_process_next_event_success(db_session):
    source = Source(
        name="orchestration_success_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="orchestration_success_1",
        event_type="test_event",
        payload={"key": "value"},
        status="received",
        attempt_count=0,
        max_attempts=5
    )

    db_session.add(event)
    db_session.commit()

    def successful_handler(event,cancellation_event):
        assert event.event_type == "test_event"

    result = process_next_event(
        db_session,
        successful_handler
    )

    assert result.status == "processed"
    assert result.attempt_count == 1
    assert result.processed_at is not None


def test_process_next_event_failure(db_session):
    source = Source(
        name="orchestration_failure_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="orchestration_failure_1",
        event_type="test_event",
        payload={"key": "value"},
        status="received",
        attempt_count=0,
        max_attempts=5
    )

    db_session.add(event)
    db_session.commit()

    db_session.refresh(event)

    def failing_handler(event, cancellation_event):
        raise RuntimeError("Simulated processing failure")

    result = process_next_event(
        db_session,
        failing_handler
    )

    assert result.status == "retrying"
    assert result.attempt_count == 1
    assert result.last_error == "Simulated processing failure"
    assert result.next_retry_at is not None



def test_process_order_created_event(db_session):

    register_handlers()

    source = Source(
        name="integration_order_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="integration_order_001",
        event_type="order.created",
        payload={
            "event_id": "integration_order_001",
            "event_type": "order.created",
            "data": {
                "order_id": 101,
                "amount": 250
            }
        },
        status="received",
        attempt_count=0,
        max_attempts=5
    )

    db_session.add(event)
    db_session.commit()

    result = process_next_event(
        db_session,
        dispatch_event
    )

    assert result is not None
    assert result.status == "processed"
    assert result.attempt_count == 1
    assert result.processed_at is not None
    assert result.last_error is None
    assert result.locked_until is None
    
    
def test_process_order_created_permanent_failure(db_session):

    register_handlers()

    source = Source(
        name="permanent_failure_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="permanent_failure_001",
        event_type="order.created",
        payload={
            "event_id": "permanent_failure_001",
            "event_type": "order.created",
            "data": None
        },
        status="received",
        attempt_count=0,
        max_attempts=5
    )

    db_session.add(event)
    db_session.commit()

    result = process_next_event(
        db_session,
        dispatch_event
    )

    assert result.status == "dead_letter"
    assert result.attempt_count == 1
    assert result.last_error == "Invalid order data"
    assert result.next_retry_at is None
    assert result.locked_until is None
    
def test_failed_event_moves_to_dead_letter_after_max_attempts(db_session):

    source = Source(
        name="max_attempts_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.flush()

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="max_attempts_event",
        event_type="test_event",
        payload={"key": "value"},
        status="processing",
        attempt_count=5,
        max_attempts=5,
        claim_version=1
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    failed_event = mark_event_as_failed(
        db_session,
        event,
        "Maximum attempts reached",
        expected_claim_version=1
    )

    assert failed_event.status == "dead_letter"
    assert failed_event.last_error == "Maximum attempts reached"
    assert failed_event.next_retry_at is None
    assert failed_event.locked_until is None