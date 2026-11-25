from sqlalchemy.orm import Session
from sqlalchemy import select,and_, or_, func, update
from app.db.models.webhook_event import WebhookEvent
from datetime import datetime, timezone, timedelta
from typing import Callable
from app.core.exceptions import TransientProcessingError, PermanentProcessingError
import threading, logging
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

def claim_next_event(db : Session, lease_duration_seconds: int = 300):
    
    eligible_event = or_(
        WebhookEvent.status == "received",
        and_(
            WebhookEvent.status == "retrying",
            or_(
                WebhookEvent.next_retry_at.is_(None),
                WebhookEvent.next_retry_at <= func.now()
            )
        ),
        and_(
            WebhookEvent.status == "processing",
            or_(
                WebhookEvent.locked_until.is_(None),
                WebhookEvent.locked_until <= func.now()
            )
        )
    )
    
    statement = (
        select(WebhookEvent)
        .where(eligible_event)
        .order_by(WebhookEvent.received_at.asc())
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    event = db.execute(statement).scalar_one_or_none()
    
    if event is None:
        return None
    
    event.status = "processing"
    event.attempt_count += 1
    event.claim_version += 1
    event.locked_until = func.now() + timedelta(seconds=lease_duration_seconds)
    db.commit()
    db.refresh(event)
    
    return event

def renew_event_lease(db: Session,event: WebhookEvent,expected_claim_version: int,lease_duration_seconds: int = 300):
    
    statement = (
        update(WebhookEvent)
        .where(
            WebhookEvent.id == event.id,
            WebhookEvent.status == "processing",
            WebhookEvent.claim_version == expected_claim_version,
            WebhookEvent.locked_until > func.now()
        )
        .values(
            locked_until=func.now() + timedelta(
                seconds=lease_duration_seconds
            )
        )
        .execution_options(synchronize_session=False)
    )

    result = db.execute(statement)

    if result.rowcount != 1:
        db.rollback()
        raise RuntimeError(
            "Cannot renew lease: worker no longer owns this event"
        )

    db.commit()
    db.refresh(event)

    return event

def start_lease_heartbeat(event_id,claim_version: int,lease_duration_seconds: int = 300,renewal_interval_seconds: int = 60):
    stop_event = threading.Event()
    heartbeat_failed = threading.Event()

    def heartbeat():
        while not stop_event.wait(renewal_interval_seconds):
            try:
                with SessionLocal() as db:
                    
                    event = db.get(WebhookEvent, event_id)

                    if event is None:
                        logger.warning(
                            "Heartbeat stopped: event %s not found",
                            event_id
                        )
                        break

                    renew_event_lease(
                        db,
                        event,
                        expected_claim_version=claim_version,
                        lease_duration_seconds=lease_duration_seconds
                    )

                    logger.debug(
                        "Lease renewed for event %s",
                        event_id
                    )

            except RuntimeError:
                logger.warning(
                    "Heartbeat stopped: worker lost ownership of event %s",
                    event_id
                )
                heartbeat_failed.set()
                break

            except Exception:
                logger.exception(
                    "Unexpected heartbeat error for event %s",
                    event_id
                )
                heartbeat_failed.set()
                break

    heartbeat_thread = threading.Thread(
        target=heartbeat,
        daemon=True
    )

    heartbeat_thread.start()

    return stop_event, heartbeat_thread,heartbeat_failed

def mark_event_as_processed(db: Session, event: WebhookEvent, expected_claim_version : int):
    
    statement  = (
        update(WebhookEvent)
        .where(
            WebhookEvent.id == event.id,
            WebhookEvent.status == "processing",
            WebhookEvent.claim_version == expected_claim_version
        )
        .values(
            status="processed",
            processed_at=func.now(),
            next_retry_at=None,
            last_error=None,
            locked_until=None
        )
        .execution_options(synchronize_session=False)
    )
    result = db.execute(statement)
    
    if result.rowcount !=1: 
        db.rollback()
        raise RuntimeError(
            "Cannot mark event as processed: worker no longer owns this event"
        )
        
    db.commit()
    db.refresh(event)
   
    return event
    
    
def mark_event_as_failed(db: Session, event: WebhookEvent, error: str, expected_claim_version : int):
    
    
    if event.attempt_count >= event.max_attempts:
        event.status = "dead_letter"
        event.next_retry_at = None
    else:
        delays_second = min(2 ** event.attempt_count, 3600)
        
        event.status = "retrying"
        event.next_retry_at = func.now() + timedelta(seconds=delays_second)
        
        
    statement = (
        update(WebhookEvent)
        .where(
            WebhookEvent.id == event.id,
            WebhookEvent.status == "processing",
            WebhookEvent.claim_version == expected_claim_version
        )
        .values(
            status=event.status,
            last_error=str(error)[:2000],
            next_retry_at=event.next_retry_at,
            locked_until=None
        )
        .execution_options(synchronize_session=False)
    )
    
    result = db.execute(statement)
    
    if result.rowcount != 1:
        db.rollback()
        raise RuntimeError(
            "Cannot mark event as failed: worker no longer owns this event"
        )
    
    db.commit()
    db.refresh(event)
    
    return event

def mark_event_as_dead_letter(db : Session,event : WebhookEvent,error : str, expected_claim_version : int):
    
    statement = (
        update(WebhookEvent)
        .where(
            WebhookEvent.id == event.id,
            WebhookEvent.status == "processing",
            WebhookEvent.claim_version == expected_claim_version
        )
        .values(
            status="dead_letter",
            last_error=str(error)[:2000],
            next_retry_at=None,
            locked_until=None
        )
        .execution_options(synchronize_session=False)
    )
    
    result = db.execute(statement)
    
    if result.rowcount != 1:
        db.rollback()
        raise RuntimeError(
            "Cannot mark event as dead letter: worker no longer owns this event"
        )

    db.commit()
    db.refresh(event)

    return event


def process_next_event(db : Session,handler : Callable[[WebhookEvent], None]):
    
    event = claim_next_event(db)
    
    if event is None:
        return None
    
    expected_claim_version = event.claim_version
    
    stop_event, heartbeat_thread, heartbeat_failed = start_lease_heartbeat(
        event_id=event.id,
        claim_version=expected_claim_version,
        lease_duration_seconds=300,
        renewal_interval_seconds=60
    )
    
    permanent_error = None
    processing_error = None
    
    try:
        handler(event,heartbeat_failed)
        
    except PermanentProcessingError as e:
        permanent_error = str(e)

    except TransientProcessingError as e:
        processing_error = str(e)

    except Exception as e:
        processing_error = str(e)

    finally:
        stop_event.set()
        heartbeat_thread.join()
        
    if heartbeat_failed.is_set():
        raise RuntimeError(
            f"Heartbeat failed while processing event {event.id}. "
            "Worker cannot safely finalize this event."
        )
        
    if permanent_error is not None:
        return mark_event_as_dead_letter(
            db,
            event,
            permanent_error,
            expected_claim_version
        )

    if processing_error is not None:
        return mark_event_as_failed(
            db,
            event,
            processing_error,
            expected_claim_version
        )

    return mark_event_as_processed(
        db,
        event,
        expected_claim_version
    )

