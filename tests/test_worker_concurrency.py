
from datetime import datetime, timedelta, timezone
from sqlalchemy import select
from app.db.models.source import Source
from app.db.models.webhook_event import WebhookEvent
from app.services.webhook_worker import claim_next_event
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier


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
