import uuid
from datetime import datetime
from sqlalchemy import String,Boolean,DateTime,text,func

from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base


class Source(Base):
    
    __tablename__="sources"
    id : Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    signing_secret_encrypted : Mapped[str] = mapped_column(
        String(500),
        nullable=True
    )
    
    name : Mapped[str] = mapped_column(
        String(100),
        unique=True,
        nullable=False
    )
    is_active : Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default=text("true"),
        nullable=False
    )
    created_at : Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable= False
    )