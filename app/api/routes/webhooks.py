import uuid

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert

from app import db
from app.api.deps import DBsession
from app.core.security import decrypt_secret
from app.db.models import Source, WebhookEvent
from app.schemas.webhook import WebhookPayload
from app.services.signature import verify_signature
from typing import Annotated
from fastapi import Depends

router = APIRouter(
    prefix="/webhooks",
    tags=["Webhooks"]
)

async def get_raw_body(request : Request) -> bytes:
    return await request.body()
    
RawBody = Annotated[bytes, Depends(get_raw_body)]

@router.post("/{source_id}")
def receive_webhook(source_id : uuid.UUID,request : Request,raw_body : RawBody,db : DBsession):
    
    source = db.scalar(
        select(Source).where(Source.id == source_id)
    )
     # 1. Find the source
    if not source:
        raise HTTPException(
            status_code = 404,
            detail = f"Source with id {source_id} not found"
        )
        
    if not source.is_active:
        raise HTTPException(
            status_code = 403,
            detail = f"Source with id {source_id} is not active"
        )
        
    # 2. Verify the signature
    signature = request.headers.get("X-Webhook-Signature")
    
    if not signature:
        raise HTTPException(
            status_code = 400,
            detail = "Missing X-Webhook-Signature header"
        )
        
    # 3. Decrypt the source secret
        
    try:
        secret = decrypt_secret(source.signing_secret_encrypted)
    except Exception as e:
        raise HTTPException(
            status_code = 500,
            detail = "Unable to retrieve signing secret"
        )
        
    # 4. Verify the signature
    if not verify_signature(raw_body, signature, secret):
        raise HTTPException(
            status_code = 401,
            detail = "Invalid webhook signature"
        )
        
    # 5 validate json structure
    try:
        payload = WebhookPayload.model_validate_json(raw_body)
    except ValidationError as e:
        raise HTTPException(
            status_code = 422,
            detail = f"Invalid payload structure: {e.errors()}"
        )
        
    # 6. Store the webhook event
    statement = (
        insert(WebhookEvent)
        .values(
            source_id=source.id,
            external_event_id=payload.event_id,
            event_type=payload.event_type,
            payload=payload.model_dump()
        )
        .on_conflict_do_nothing(
            constraint="uq_source_external_event_id"
        )
        .returning(WebhookEvent.id)
    )
    
    inserted_event_id = db.execute(statement).scalar_one_or_none()
    db.commit()

    if inserted_event_id is None:
        return {
        "status": "duplicate",
        "message": "Event has already been received"
    }

    return {
        "status": "received",
        "event_id": str(inserted_event_id),
        "external_event_id": payload.event_id
    }
    