from sqlalchemy.orm import Session
from sqlalchemy import select,and_, or_, func
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
    event.locked_until = datetime.now(timezone.utc) + timedelta(minutes=5)
    db.commit()
    db.refresh(event)
    
    return event

def mark_event_as_processed(db: Session, event: WebhookEvent):
    
    if event.status != "processing":
        raise ValueError("Event must be in 'processing' status to mark as processed.")
    
    event.status = "processed"
    event.processed_at = datetime.now(timezone.utc)
    event.next_retry_at = None
    event.last_error = None
    event.locked_until = None
    
    db.commit()
    db.refresh(event)
    
    return event
    
    
def mark_event_as_failed(db: Session, event: WebhookEvent, error: str):
    
    if event.status != "processing":
        raise ValueError("Event must be in 'processing' status to mark as failed.")
    
    event.last_error = error[:2000]
    
    if event.attempt_count >= event.max_attempts:
        event.status = "dead_letter"
        event.next_retry_at = None
    else:
        delays_second = min(2 ** event.attempt_count, 3600)
        
        event.status = "retrying"
        event.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delays_second)
    
    event.locked_until = None
    db.commit()
    db.refresh(event)
    
    return event


def mark_event_as_dead_letter(db,event,error):
    
    if event.status !="processing":
        raise ValueError(
            "Event must be in 'processing' status to mark as dead letter."
        )
    
    event.status = "dead_letter"
    event.last_error = str(error)[:2000]
    event.next_retry_at = None
    event.locked_until = None

    db.commit()
    db.refresh(event)

    return event


def process_next_event(db : Session,handler : Callable[[WebhookEvent], None]):
    
    event = claim_next_event(db)
    
    if event is None:
        return None
    
    try:
        handler(event)
        
    except PermanentProcessingError as e:
        return mark_event_as_dead_letter(
            db,
            event,
            str(e)
        )
    except TransientProcessingError as e:
        return mark_event_as_failed(
            db,
            event,
            str(e)
        )
    except Exception as e:
        return mark_event_as_failed(
            db,
            event,
            str(e)
        )
    
    return mark_event_as_processed(db, event)