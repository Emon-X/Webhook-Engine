from sqlalchemy.orm import Session
from app.db.models.event_replay_log import EventReplayLog
from app.db.models.webhook_event import WebhookEvent


def event_replay(db : Session, event_id, replayed_by : str | None = None):
    
    event = db.query(WebhookEvent).filter(WebhookEvent.id == event_id).with_for_update().first()
    
    if event is None:
        raise LookupError(f"No event found with id {event_id}")
    
    if event.status !="dead_letter":
        raise ValueError(
            "Only dead-letter events can be replayed"
        )
        
    try:
        replay_log = EventReplayLog(
            event_id=event.id,
            previous_status=event.status,
            previous_attempt_count=event.attempt_count,
            previous_error=event.last_error,
            replayed_by=replayed_by
        )
        db.add(replay_log)
        
        event.status = "retrying"
        event.attempt_count = 0
        event.next_retry_at = None
        event.locked_until = None
        event.last_error = None
        event.processed_at = None
            
        db.commit()
        db.refresh(event)
        
    except Exception:
        db.rollback()
        raise
    

    return event