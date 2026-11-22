from app.core.config import settings
from cryptography.fernet import Fernet

fernet = Fernet(settings.FERNET_KEY.encode())

def encrypt_secret(secret: str) -> str:
    
    encrypted = fernet.encrypt(secret.encode())
    return encrypted.decode()

def decrypt_secret(encrypted_secret: str) -> str:
    
    decrypted = fernet.decrypt(encrypted_secret.encode())
    return decrypted.decode()