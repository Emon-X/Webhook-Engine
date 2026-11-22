
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func,update
from app.db.models.source import Source
from app.db.models.webhook_event import WebhookEvent
from app.services.webhook_worker import claim_next_event, mark_event_as_processed
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import pytest


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