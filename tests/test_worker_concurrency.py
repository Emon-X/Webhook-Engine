
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func,update
from app.db.models.source import Source
from app.db.models.webhook_event import WebhookEvent
from app.services.webhook_worker import claim_next_event, mark_event_as_processed, renew_event_lease
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import pytest
import time
from threading import Event
from app.services.webhook_worker import process_next_event
from app.services import webhook_worker


def test_worker_skips_event_locked_by_another_session(independent_session_factory):
    
    SessionFactory = independent_session_factory

    # Create test data using an independent database session.
    with SessionFactory() as db:
        
        source = Source(
            name="concurrency_test_source",
            signing_secret_encrypted=None
        )

        db.add(source)
        db.commit()
        db.refresh(source)

        now = datetime.now(timezone.utc)

        event_a = WebhookEvent(
            source_id=source.id,
            external_event_id="concurrency_event_a",
            event_type="order.created",
            payload={"data": {"order_id": 1}},
            status="received",
            received_at=now - timedelta(minutes=2)
        )

        event_b = WebhookEvent(
            source_id=source.id,
            external_event_id="concurrency_event_b",
            event_type="order.created",
            payload={"data": {"order_id": 2}},
            status="received",
            received_at=now - timedelta(minutes=1)
        )

        db.add_all([event_a, event_b])
        db.commit()

        event_a_id = event_a.id
        event_b_id = event_b.id

    # Open two independent PostgreSQL sessions.
    session_a = SessionFactory()
    session_b = SessionFactory()

    try:
        # Worker A locks the oldest event.
        locked_event = session_a.execute(
            select(WebhookEvent)
            .where(WebhookEvent.id == event_a_id)
            .with_for_update()
        ).scalar_one()

        assert locked_event.status == "received"

        # Worker B tries to claim the next available event.
        claimed_event = claim_next_event(session_b)

        # Worker B must skip event A and claim event B.
        assert claimed_event is not None
        assert claimed_event.id == event_b_id
        assert claimed_event.status == "processing"
        assert claimed_event.attempt_count == 1

    finally:
        # Release Worker A's database lock.
        session_a.rollback()

        session_a.close()
        session_b.close()




def test_two_workers_claim_different_events_concurrently(independent_session_factory):
    SessionFactory = independent_session_factory

    with SessionFactory() as db:
        source = Source(
            name="parallel_worker_source",
            signing_secret_encrypted=None
        )

        db.add(source)
        db.commit()
        db.refresh(source)

        now = datetime.now(timezone.utc)

        events = [
            WebhookEvent(
                source_id=source.id,
                external_event_id=f"parallel_event_{i}",
                event_type="order.created",
                payload={"data": {"order_id": i}},
                status="received",
                received_at=now + timedelta(seconds=i)
            )
            for i in range(2)
        ]

        db.add_all(events)
        db.commit()

    # Both threads wait here until they are ready.
    barrier = Barrier(2)

    def worker():
        with SessionFactory() as db:
            barrier.wait(timeout=10)
            event = claim_next_event(db)
            return event.id if event else None

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(worker)
        future_b = executor.submit(worker)

        event_a_id = future_a.result(timeout=15)
        event_b_id = future_b.result(timeout=15)

    assert event_a_id is not None
    assert event_b_id is not None

    # Both workers must claim different events.
    assert event_a_id != event_b_id


def test_worker_can_reclaim_event_after_lease_expires(db_session):

    source = Source(
        name="expired_lease_test_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    database_now = db_session.execute(
        select(func.now())
    ).scalar_one()

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="expired_lease_event",
        event_type="order.created",
        payload={"data": {"order_id": 101}},
        status="processing",
        attempt_count=1,
        max_attempts=5,
        locked_until=database_now - timedelta(minutes=1)
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    event_id = event.id

    # Another worker attempts to claim the expired event.
    reclaimed_event = claim_next_event(db_session)

    assert reclaimed_event is not None
    assert reclaimed_event.id == event_id
    assert reclaimed_event.status == "processing"
    assert reclaimed_event.attempt_count == 2
    assert reclaimed_event.locked_until > database_now
    
    
def test_worker_cannot_reclaim_event_with_valid_lease(db_session):

    source = Source(
        name="valid_lease_test_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    database_now = db_session.execute(
        select(func.now())
    ).scalar_one()

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="valid_lease_event",
        event_type="order.created",
        payload={"data": {"order_id": 102}},
        status="processing",
        attempt_count=1,
        max_attempts=5,
        locked_until=database_now + timedelta(minutes=5)
    )

    db_session.add(event)
    db_session.commit()
    

    # Another worker attempts to claim an event.
    claimed_event = claim_next_event(db_session)
    assert claimed_event is None
    
def test_claim_increments_claim_version(db_session):

    source = Source(
        name="claim_version_test_source",
        signing_secret_encrypted=None
    )

    db_session.add(source)
    db_session.commit()
    db_session.refresh(source)

    event = WebhookEvent(
        source_id=source.id,
        external_event_id="claim_version_event",
        event_type="order.created",
        payload={"data": {"order_id": 103}},
        status="received",
        attempt_count=0,
        claim_version=0
    )

    db_session.add(event)
    db_session.commit()
    db_session.refresh(event)

    claimed_event = claim_next_event(db_session)

    assert claimed_event.claim_version == 1
    assert claimed_event.attempt_count == 1
    assert claimed_event.status == "processing"
    
    
def test_stale_worker_cannot_mark_reclaimed_event_as_processed(independent_session_factory):
    
    SessionFactory = independent_session_factory

    # Create a source and an event.
    with SessionFactory() as db:
        source = Source(
            name="fencing_test_source",
            signing_secret_encrypted=None
        )

        db.add(source)
        db.flush()

        event = WebhookEvent(
            source_id=source.id,
            external_event_id="fencing_event_1",
            event_type="test_event",
            payload={"data": {}},
            status="received",
            attempt_count=0,
            max_attempts=5,
            claim_version=0
        )

        db.add(event)
        db.commit()

        event_id = event.id

    # Worker A claims the event.
    session_a = SessionFactory()

    try:
        event_a = claim_next_event(session_a)

        assert event_a.id == event_id
        version_a = event_a.claim_version
        assert version_a == 1

        # Simulate Worker A's lease expiring.
        session_a.execute(
            update(WebhookEvent)
            .where(WebhookEvent.id == event_id)
            .values(
                locked_until=func.now() - timedelta(minutes=1)
            )
            .execution_options(synchronize_session=False)
        )
        session_a.commit()

        # Worker B reclaims the same event.
        with SessionFactory() as session_b:
            event_b = claim_next_event(session_b)

            assert event_b.id == event_id
            version_b = event_b.claim_version
            assert version_b == 2

        # Worker A attempts to finish using its outdated token.
        with pytest.raises(RuntimeError, match="no longer owns"):
            mark_event_as_processed(
                session_a,
                event_a,
                expected_claim_version=version_a
            )

        # Verify that Worker B still owns the event.
        with SessionFactory() as verification_db:
            current_event = verification_db.execute(
                select(WebhookEvent).where(
                    WebhookEvent.id == event_id
                )
            ).scalar_one()

            assert current_event.status == "processing"
            assert current_event.claim_version == version_b

    finally:
        session_a.close()
        
def test_worker_can_renew_lease(independent_session_factory):
    """Test that a worker can renew the lease on a claimed event."""
    SessionFactory = independent_session_factory

    with SessionFactory() as db:
        source = Source(
            name="lease_renewal_source",
            signing_secret_encrypted=None
        )
        db.add(source)
        db.flush()

        event = WebhookEvent(
            source_id=source.id,
            external_event_id="lease_renewal_1",
            event_type="test_event",
            payload={"data": {}},
            status="received",
            attempt_count=0,
            max_attempts=5,
            claim_version=0
        )

        db.add(event)
        db.commit()
        event_id = event.id

    with SessionFactory() as db:
        
        event = claim_next_event(db)

        assert event.id == event_id

        original_lease = event.locked_until
        claim_version = event.claim_version

        renewed_event = renew_event_lease(
            db,
            event,
            expected_claim_version=claim_version,
            lease_duration_seconds=600
        )

        assert renewed_event.locked_until > original_lease
        assert renewed_event.claim_version == claim_version
        assert renewed_event.status == "processing"
        
def test_stale_worker_cannot_renew_lease(independent_session_factory):
    
    SessionFactory = independent_session_factory

    with SessionFactory() as db:
        source = Source(
            name="stale_lease_source",
            signing_secret_encrypted=None
        )
        db.add(source)
        db.flush()

        event = WebhookEvent(
            source_id=source.id,
            external_event_id="stale_lease_1",
            event_type="test_event",
            payload={"data": {}},
            status="received",
            attempt_count=0,
            max_attempts=5,
            claim_version=0
        )

        db.add(event)
        db.commit()
        event_id = event.id

    session_a = SessionFactory()

    try:
        # Worker A claims the event.
        event_a = claim_next_event(session_a)
        version_a = event_a.claim_version

        assert version_a == 1

        # Simulate expiration of Worker A's lease.
        session_a.execute(
            update(WebhookEvent)
            .where(WebhookEvent.id == event_id)
            .values(
                locked_until=func.now() - timedelta(minutes=1)
            )
            .execution_options(synchronize_session=False)
        )
        session_a.commit()

        # Worker B reclaims the event.
        with SessionFactory() as session_b:
            event_b = claim_next_event(session_b)
            version_b = event_b.claim_version

            assert version_b == 2

            lease_before = event_b.locked_until

        # Worker A attempts to renew using version 1.
        with pytest.raises(RuntimeError, match="no longer owns"):
            renew_event_lease(
                session_a,
                event_a,
                expected_claim_version=version_a,
                lease_duration_seconds=600
            )

        # Verify Worker B's lease was not changed.
        with SessionFactory() as verification_db:
            current_event = verification_db.execute(
                select(WebhookEvent).where(
                    WebhookEvent.id == event_id
                )
            ).scalar_one()

            assert current_event.claim_version == version_b
            assert current_event.status == "processing"
            assert current_event.locked_until == lease_before

    finally:
        session_a.close()
        
def test_heartbeat_renews_lease_during_processing(independent_session_factory,monkeypatch):
    SessionFactory = independent_session_factory

    with SessionFactory() as db:
        source = Source(
            name="heartbeat_integration_source",
            signing_secret_encrypted=None
        )

        db.add(source)
        db.flush()

        event = WebhookEvent(
            source_id=source.id,
            external_event_id="heartbeat_integration_1",
            event_type="test_event",
            payload={"data": {}},
            status="received",
            attempt_count=0,
            max_attempts=5,
            claim_version=0
        )

        db.add(event)
        db.commit()
        event_id = event.id
        
    monkeypatch.setattr(
        webhook_worker,
        "SessionLocal",
        SessionFactory
    )
    renewal_observed = Event()

    original_renew_lease = webhook_worker.renew_event_lease

    def tracked_renew_lease(*args, **kwargs):
        result = original_renew_lease(*args, **kwargs)
        renewal_observed.set()
        return result

    monkeypatch.setattr(    
        webhook_worker,
        "renew_event_lease",
        tracked_renew_lease
    )

    # Use short intervals to keep the test fast.
    original_start_heartbeat = webhook_worker.start_lease_heartbeat
    original_claim_next_event = webhook_worker.claim_next_event
    
    monkeypatch.setattr(
        webhook_worker,
        "claim_next_event",
        lambda db: original_claim_next_event(
            db,
            lease_duration_seconds=2
        )
    )

    monkeypatch.setattr(
        webhook_worker,
        "start_lease_heartbeat",
        lambda event_id, claim_version, **kwargs:
            original_start_heartbeat(
                event_id,
                claim_version,
                lease_duration_seconds=2,
                renewal_interval_seconds=0.2
            )
    )
    
    handler_started = Event()
    handler_finished = Event()
    observed_lease = {}

    def slow_handler(event,cancellation_event):
        handler_started.set()

        with SessionFactory() as check_db:
            current_event = check_db.get(WebhookEvent, event_id)
            initial_lease = current_event.locked_until

        observed_lease["initial"] = initial_lease
        
         # Wait until the heartbeat successfully renews the lease.
        assert renewal_observed.wait(timeout=3), (
            "Heartbeat did not renew the lease within 3 seconds"
        )

        with SessionFactory() as check_db:
            current_event = check_db.get(WebhookEvent, event_id)
            observed_lease["renewed"] = current_event.locked_until


        handler_finished.set()

    with SessionFactory() as db:
        result = process_next_event(db, slow_handler)

    assert handler_started.is_set()
    assert handler_finished.is_set()
    assert result.status == "processed"

    # The event was processed after the heartbeat had time to renew it.
    assert observed_lease["renewed"] > observed_lease["initial"]
    
def test_worker_does_not_finalize_after_heartbeat_failure(independent_session_factory,monkeypatch):
    
    SessionFactory = independent_session_factory

    # Create an event that the worker can claim.
    with SessionFactory() as db:
        source = Source(
            name="heartbeat_failure_source",
            signing_secret_encrypted=None
        )

        db.add(source)
        db.flush()

        event = WebhookEvent(
            source_id=source.id,
            external_event_id="heartbeat_failure_1",
            event_type="test_event",
            payload={"data": {}},
            status="received",
            attempt_count=0,
            max_attempts=5,
            claim_version=0
        )

        db.add(event)
        db.commit()
        event_id = event.id

    # Make the heartbeat use the test database.
    monkeypatch.setattr(
        webhook_worker,
        "SessionLocal",
        SessionFactory
    )

    # Use a short initial lease for this test.
    original_claim_next_event = webhook_worker.claim_next_event

    monkeypatch.setattr(
        webhook_worker,
        "claim_next_event",
        lambda db: original_claim_next_event(
            db,
            lease_duration_seconds=2
        )
    )

    # Configure a short heartbeat interval.
    original_start_heartbeat = webhook_worker.start_lease_heartbeat

    monkeypatch.setattr(
        webhook_worker,
        "start_lease_heartbeat",
        lambda event_id, claim_version, **kwargs:
            original_start_heartbeat(
                event_id,
                claim_version,
                lease_duration_seconds=2,
                renewal_interval_seconds=0.2
            )
    )

    renewal_attempted = Event()

    # Simulate a failed lease renewal.
    def failing_renewal(*args, **kwargs):
        renewal_attempted.set()
        raise RuntimeError("Simulated lease renewal failure")

    monkeypatch.setattr(
        webhook_worker,
        "renew_event_lease",
        failing_renewal
    )

    # Keep the handler running until the heartbeat attempts renewal.
    def slow_handler(event, cancellation_event):
        assert renewal_attempted.wait(timeout=3), (
            "Heartbeat did not attempt lease renewal"
        )

    # The worker must refuse to finalize the event.
    with SessionFactory() as db:
        with pytest.raises(RuntimeError,match="Heartbeat failed"):
            process_next_event(db, slow_handler)

    # Verify that the event was not marked as processed.
    with SessionFactory() as db:
        current_event = db.get(WebhookEvent, event_id)

        assert current_event.status == "processing"
        assert current_event.processed_at is None
        assert current_event.claim_version == 1
        
def test_worker_cannot_renew_expired_lease(independent_session_factory):
    SessionFactory = independent_session_factory

    with SessionFactory() as db:
        source = Source(
            name="expired_lease_renewal_source",
            signing_secret_encrypted=None
        )

        db.add(source)
        db.flush()

        event = WebhookEvent(
            source_id=source.id,
            external_event_id="expired_lease_renewal_1",
            event_type="test_event",
            payload={"data": {}},
            status="processing",
            attempt_count=1,
            max_attempts=5,
            claim_version=1,
            locked_until=datetime.now(timezone.utc) - timedelta(minutes=1)
        )

        db.add(event)
        db.commit()
        event_id = event.id

    with SessionFactory() as db:
        event = db.get(WebhookEvent, event_id)

        with pytest.raises(RuntimeError,match="Cannot renew lease"):
            renew_event_lease(
                db,
                event,
                expected_claim_version=1
            )

    with SessionFactory() as db:
        event = db.get(WebhookEvent, event_id)

        assert event.status == "processing"
        assert event.claim_version == 1
        
        
def test_handler_receives_cancellation_signal(independent_session_factory,monkeypatch):
    SessionFactory = independent_session_factory

    with SessionFactory() as db:
        source = Source(
            name="cancellation_test_source",
            signing_secret_encrypted=None
        )

        db.add(source)
        db.flush()

        event = WebhookEvent(
            source_id=source.id,
            external_event_id="cancellation_test_1",
            event_type="test_event",
            payload={"data": {}},
            status="received",
            attempt_count=0,
            max_attempts=5,
            claim_version=0
        )

        db.add(event)
        db.commit()
        event_id = event.id

    monkeypatch.setattr(
        webhook_worker,
        "SessionLocal",
        SessionFactory
    )

    original_claim_next_event = webhook_worker.claim_next_event

    monkeypatch.setattr(
        webhook_worker,
        "claim_next_event",
        lambda db: original_claim_next_event(
            db,
            lease_duration_seconds=2
        )
    )

    original_start_heartbeat = webhook_worker.start_lease_heartbeat

    monkeypatch.setattr(
        webhook_worker,
        "start_lease_heartbeat",
        lambda event_id, claim_version, **kwargs:
            original_start_heartbeat(
                event_id,
                claim_version,
                lease_duration_seconds=2,
                renewal_interval_seconds=0.2
            )
    )

    def failing_renewal(*args, **kwargs):
        raise RuntimeError("Simulated heartbeat failure")

    monkeypatch.setattr(
        webhook_worker,
        "renew_event_lease",
        failing_renewal
    )

    cancellation_observed = Event()

    def handler(event, cancellation_event):
        assert cancellation_event.wait(timeout=3)
        cancellation_observed.set()

    with SessionFactory() as db:
        with pytest.raises(RuntimeError, match="Heartbeat failed"):
            process_next_event(db, handler)

    assert cancellation_observed.is_set()

    with SessionFactory() as db:
        current_event = db.get(WebhookEvent, event_id)
        assert current_event.status == "processing"
        
def test_handler_can_check_cancellation_before_work():
    cancellation_event = Event()
    cancellation_event.set()

    work_started = False

    def handler(cancellation_event):
        nonlocal work_started

        if cancellation_event.is_set():
            return

        work_started = True

    handler(cancellation_event)

    assert work_started is False