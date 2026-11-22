
import uuid
from datetime import datetime
from sqlalchemy import String,Integer, Text,  DateTime, ForeignKey,func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base


class EventReplayLog(Base):

    __tablename__ = "event_replay_logs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    event_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("webhook_events.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    previous_status: Mapped[str] = mapped_column(
        String(100),
        nullable=False
    )

    previous_attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False
    )

    previous_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True
    )

    replayed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )

    replayed_by: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True
    )
