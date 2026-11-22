from uuid import UUID
from pydantic import BaseModel, Field

class WebhookPayload(BaseModel):
    event_id : str = Field(
        min_length=2,
        max_length=100
    )
    event_type: str = Field(
        min_length=2,
        max_length=100
    )
    data: dict
    
class EventReplayResponse(BaseModel):
    
    event_id: UUID
    status: str
    message: str