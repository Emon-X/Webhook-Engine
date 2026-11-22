from app.db.session import get_db
from sqlalchemy.orm import Session
from typing import Annotated
from fastapi import Depends


DBsession = Annotated[Session, Depends(get_db)] 