from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String,DateTime, UniqueConstraint,Text,text,func,ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
from datetime import datetime
import uuid
from typing import Any
from app.db.base import Base


class WebhookEvent(Base):
    
    __tablename__ = "webhook_events"
    __table_args__ = (UniqueConstraint(
        "source_id",
        "external_event_id",
        name = "uq_source_external_event_id"
        ),
    )
                      
    id : Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )
    source_id : Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("sources.id"),
        nullable=False
    )
    external_event_id : Mapped[str] = mapped_column(
        String(500),
        nullable=False
    )
    event_type : Mapped[str] = mapped_column(
        String(100),
        nullable=False
    )
    payload : Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False
    )
    received_at : Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    status : Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        default="received",
        server_default=text("'received'")
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0")
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default = 5,
        server_default=text("5")
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True
    )
    locked_until : Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True
    )
    claim_version : Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0")
    )