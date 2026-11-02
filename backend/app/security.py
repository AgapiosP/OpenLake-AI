from datetime import datetime, timedelta, timezone
from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session
from sqlalchemy import select
import jwt
from argon2 import PasswordHasher, exceptions
from cryptography.fernet import Fernet
from .config import get_settings
from .db import get_db
from .models import User

ph = PasswordHasher()

def hash_password(password: str) -> str:
    return ph.hash(password)

def check_password(hashed: str, password: str) -> bool:
    try:
        return ph.verify(hashed, password)
    except (exceptions.VerifyMismatchError, exceptions.VerificationError, exceptions.InvalidHashError):
        return False

def make_token(user: User) -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode({'sub': user.id, 'iat': now, 'exp': now + timedelta(hours=12)}, get_settings().secret_key, algorithm='HS256')

def current_user(request: Request, db: Session = Depends(get_db)) -> User:
    token = request.cookies.get('openlake_session')
    if not token:
        raise HTTPException(401, 'Authentication required')
    try:
        user_id = jwt.decode(token, get_settings().secret_key, algorithms=['HS256'], options={'require': ['exp', 'sub']})['sub']
    except jwt.PyJWTError:
        raise HTTPException(401, 'Invalid session') from None
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(401, 'Unknown user')
    return user

def cipher() -> Fernet:
    key = get_settings().encryption_key
    if not key:
        raise HTTPException(503, 'AI encryption is not configured')
    return Fernet(key.encode())
