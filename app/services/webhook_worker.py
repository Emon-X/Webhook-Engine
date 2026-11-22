from sqlalchemy.orm import Session
from sqlalchemy import select,and_, or_, func, update
from app.db.models.webhook_event import WebhookEvent
from datetime import datetime, timezone, timedelta
from typing import Callable
from app.core.exceptions import TransientProcessingError, PermanentProcessingError

def claim_next_event(db : Session):
    
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
    event.locked_until = datetime.now(timezone.utc) + timedelta(minutes=5)
    db.commit()
    db.refresh(event)
    
    return event

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
        event.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delays_second)
        
        
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
    
    try:
        handler(event)
        
    except PermanentProcessingError as e:
        return mark_event_as_dead_letter(
            db,
            event,
            str(e),
            expected_claim_version
        )
    except TransientProcessingError as e:
        return mark_event_as_failed(
            db,
            event,
            str(e),
            expected_claim_version
        )
    except Exception as e:
        return mark_event_as_failed(
            db,
            event,
            str(e),
            expected_claim_version
        )
    
    return mark_event_as_processed(db, event, expected_claim_version)