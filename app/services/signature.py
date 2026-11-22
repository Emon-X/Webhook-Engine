import hmac
import hashlib

def verify_signature(payload : bytes, signature: str, secret : str) -> bool:
    
    if not signature.startswith("sha256="):
        return False
    
    expected_signature = hmac.new(
        key=secret.encode('utf-8'),
        msg=payload,
        digestmod=hashlib.sha256
    ).hexdigest()
    
    expected_header = f"sha256={expected_signature}"
    
    return hmac.compare_digest(expected_header, signature)