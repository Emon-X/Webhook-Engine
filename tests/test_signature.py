import hmac, hashlib
from app.services.signature import verify_signature

secret = "payment-secret-value-123456"
payload = b'{"event_id":"evt_1001","event_type":"payment.success"}'

def generate_signature(payload: bytes, secret: str) -> str:
    
    signature = hmac.new(
        key=secret.encode('utf-8'),
        msg=payload,
        digestmod=hashlib.sha256
    ).hexdigest()
    
    return f"sha256={signature}"
    
def test_valid_signature():
    
    signature = generate_signature(payload, secret)
    
    assert verify_signature(payload, signature, secret) is True
    
def test_invalid_signature():
    
    result = verify_signature(
        payload,
        "sha256=invalidsignature",
        secret
    )
    
    assert result is False
    
def test_modified_payload():
    
    signature = generate_signature(payload, secret)
    
    modified_payload = b'{"event_id":"evt_1001","event_type":"payment.failed"}'
    
    result = verify_signature(modified_payload, signature, secret)
    
    assert result is False
    
def test_wrong_secret():
    
    signature = generate_signature(payload, secret)
    wrong_secret = "wrong-secret-value-654321"
    
    result = verify_signature(payload, signature, wrong_secret)
    
    assert result is False
    
    
def test_invalid_format():
    
    result = verify_signature(
        payload,
        "invalidformat",
        secret
    )
    
    assert result is False