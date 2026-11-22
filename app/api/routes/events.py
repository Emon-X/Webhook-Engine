from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import uuid
from app.schemas.webhook import EventReplayResponse
from app.services.event_replay import event_replay
from app.api.deps import DBsession


router = APIRouter(
    prefix="/events",
    tags=["Events"]
)

@router.post("/{event_id}/replay",response_model=EventReplayResponse)
def replay_webhook_event(event_id: uuid.UUID, db: DBsession):
    
    try : 
        event = event_replay(db, event_id)
    except LookupError:
        raise HTTPException(
            status_code=404,
            detail="Webhook event not found"
        )
    except ValueError as e:
        raise HTTPException(
            status_code=409,
            detail=str(e)
        )
    
    return {
        "event_id": event.id,
        "status": event.status,
        "message": "Event scheduled for replay"
    }