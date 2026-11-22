import hashlib
import hmac
from app.core.security import encrypt_secret
from app.db.models import Source
from sqlalchemy import select, func
from app.db.models import WebhookEvent

secret = "payment_gateway_123456"

def generate_signature(payload: str) -> str:
    
    digest = hmac.new(
        secret.encode('utf-8'),
        payload,
        hashlib.sha256
    ).hexdigest()
    
    return f"sha256={digest}"


def create_test_source(db):
    
    source = Source(
        name="Test Source",
        signing_secret_encrypted=encrypt_secret(secret),
        is_active=True
    )
    db.add(source)
    db.commit()
    db.refresh(source)
    return source


def test_receive_valid_webhook(client,db_session):
    
    payload = (
        b'{"event_id":"evt_test_001",'
        b'"event_type":"payment.success",'
        b'"data":{"amount":500,"currency":"USD"}}'
    )
    
    source = create_test_source(db_session)
    
    headers = {
        "X-Webhook-Signature": generate_signature(payload),
        "Content-Type": "application/json"
    }
    
    response = client.post(
        f"/webhooks/{source.id}",
        content=payload,
        headers=headers
    )
   
    
    assert response.status_code == 200
    assert response.json()["status"] == "received"
    assert response.json()["external_event_id"] == "evt_test_001"
    
    
def test_duplicate_webhook_returns_duplicate(client,db_session):
    
    payload = (
        b'{"event_id":"evt_test_002",'
        b'"event_type":"payment.success",'
        b'"data":{"amount":1000,"currency":"USD"}}'
    )
    
    source = create_test_source(db_session)
    
    headers = {
        "X-Webhook-Signature": generate_signature(payload),
        "Content-Type": "application/json"
    }
    
    # First request
    response1 = client.post(
        f"/webhooks/{source.id}",
        content=payload,
        headers=headers
    )
    
    assert response1.status_code == 200
    assert response1.json()["status"] == "received"
    
    # Second request (duplicate)
    response2 = client.post(
        f"/webhooks/{source.id}",
        content=payload,
        headers=headers
    )
    
    assert response2.status_code == 200
    assert response2.json()["status"] == "duplicate"
    
def test_invalid_signature_returns_401(client,db_session):
    
    payload = (
        b'{"event_id":"evt_test_003",'
        b'"event_type":"payment.success",'
        b'"data":{"amount":2000,"currency":"USD"}}'
    )
    
    source = create_test_source(db_session)
    
    headers = {
        "X-Webhook-Signature": "sha256=invalidsignature",
        "Content-Type": "application/json"
    }
    
    response = client.post(
        f"/webhooks/{source.id}",
        content=payload,
        headers=headers
    )
    
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid webhook signature"
    
def test_missing_signature_returns_400(client,db_session):
    
    payload = (
        b'{"event_id":"evt_test_004",'
        b'"event_type":"payment.success",'
        b'"data":{"amount":3000,"currency":"USD"}}'
    )
    
    source = create_test_source(db_session)
    
    headers = {
        "Content-Type": "application/json"
    }
    
    response = client.post(
        f"/webhooks/{source.id}",
        content=payload,
        headers=headers
    )
    
    assert response.status_code == 400
    assert response.json()["detail"] == "Missing X-Webhook-Signature header"
    
import uuid

def test_unknown_source_returns_404(client):
    
    unknown_source_id = uuid.uuid4()
    
    payload = (
        b'{"event_id":"evt_test_005",'
        b'"event_type":"payment.success",'
        b'"data":{"amount":4000,"currency":"USD"}}'
    )
    
    headers = {
        "X-Webhook-Signature": generate_signature(payload),
        "Content-Type": "application/json"
    }
    
    response = client.post(
        f"/webhooks/{unknown_source_id}",
        content=payload,
        headers=headers
    )
    
    assert response.status_code == 404
    
def test_inactive_source_returns_403(client,db_session):
    
    payload = (
        b'{"event_id":"evt_test_006",'
        b'"event_type":"payment.success",'
        b'"data":{"amount":5000,"currency":"USD"}}'
    )
    
    source = create_test_source(db_session)
    source.is_active = False
    db_session.commit()
  
    headers = {
        "X-Webhook-Signature": generate_signature(payload),
        "Content-Type": "application/json"
    }
    
    response = client.post(
        f"/webhooks/{source.id}",
        content=payload,
        headers=headers
    )
    
    assert response.status_code == 403
    
    

def test_invalid_payload_returns_422(client, db_session):
    payload = (
        b'{"event_id":"evt_invalid_payload",'
        b'"data":{"amount":500,"currency":"USD"}}'
    )

    source = create_test_source(db_session)

    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Signature": generate_signature(payload)
    }

    response = client.post(
        f"/webhooks/{source.id}",
        content=payload,
        headers=headers
    )

    assert response.status_code == 422


def test_malformed_json_returns_422(client, db_session):
    payload = b'{"event_id": "evt_bad_json", invalid json}'

    source = create_test_source(db_session)

    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Signature": generate_signature(payload)
    }

    response = client.post(
        f"/webhooks/{source.id}",
        content=payload,
        headers=headers
    )

    assert response.status_code == 422



def test_valid_webhook_creates_one_event(client, db_session):
    payload = (
        b'{"event_id":"evt_persistence_001",'
        b'"event_type":"payment.success",'
        b'"data":{"amount":500,"currency":"USD"}}'
    )

    source = create_test_source(db_session)

    response = client.post(
        f"/webhooks/{source.id}",
        content=payload,
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Signature": generate_signature(payload)
        }
    )

    event_count = db_session.scalar(
        select(func.count()).select_from(WebhookEvent).where(
            WebhookEvent.source_id == source.id
        )
    )

    assert response.status_code == 200
    assert event_count == 1


def test_invalid_signature_does_not_create_event(client, db_session):
    payload = (
        b'{"event_id":"evt_rejected_001",'
        b'"event_type":"payment.success",'
        b'"data":{"amount":500,"currency":"USD"}}'
    )

    source = create_test_source(db_session)

    response = client.post(
        f"/webhooks/{source.id}",
        content=payload,
        headers={
            "Content-Type": "application/json",
            "X-Webhook-Signature": "sha256=invalid"
        }
    )

    event_count = db_session.scalar(
        select(func.count()).select_from(WebhookEvent).where(
            WebhookEvent.source_id == source.id
        )
    )

    assert response.status_code == 401
    assert event_count == 0
