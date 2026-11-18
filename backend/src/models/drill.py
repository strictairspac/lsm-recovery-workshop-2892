from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, JSON, ForeignKey, DateTime, func
from ..config.database import Base
from ..constants.enums import DRILL_STATUSES


class Drill(Base):
    """Durability drill state. The log/checkpoint files live under /artifacts/drills."""

    __tablename__ = 'drills'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    dataset_id: Mapped[int] = mapped_column(ForeignKey('datasets.id'))
    status: Mapped[str] = mapped_column(String(20), default=DRILL_STATUSES[0])
    segment_limit: Mapped[int] = mapped_column(default=8)
    checkpoint_lsn: Mapped[int] = mapped_column(default=0)
    last_lsn: Mapped[int] = mapped_column(default=0)
    group_count: Mapped[int] = mapped_column(default=0)
    last_report: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    last_action: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
