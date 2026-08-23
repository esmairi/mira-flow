"""SQLAlchemy ORM for §6 (SQLite type mapping: UUID->String, JSONB->JSON,
TIMESTAMPTZ->DateTime UTC)."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON


def _new_uuid() -> str:
    return str(uuid.uuid4())


class Base(DeclarativeBase):
    pass


class WorkflowRun(Base):
    """One chain of jobs triggered together, from PENDING to a terminal status."""

    __tablename__ = "workflow_run"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_new_uuid)
    workflow_name: Mapped[str] = mapped_column(String, nullable=False)
    workflow_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    idempotency_key: Mapped[str | None] = mapped_column(String, unique=True)
    correlation_id: Mapped[str | None] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, nullable=False)
    input: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    output: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    lock_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    task_runs: Mapped[list["TaskRun"]] = relationship(back_populates="workflow_run")

    __table_args__ = (
        Index(
            "ix_workflow_run_status_active",
            "status",
            sqlite_where=status.in_(["PENDING", "RUNNING"]),
        ),
    )


class TaskRun(Base):
    """One attempt at one job within a WorkflowRun."""

    __tablename__ = "task_run"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=_new_uuid)
    workflow_run_id: Mapped[str] = mapped_column(
        String, ForeignKey("workflow_run.id", ondelete="CASCADE"), nullable=False
    )
    task_name: Mapped[str] = mapped_column(String, nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String, nullable=False)
    input: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    output: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    correlation_id: Mapped[str | None] = mapped_column(String)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime)
    started_at: Mapped[datetime | None] = mapped_column(DateTime)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime)

    workflow_run: Mapped[WorkflowRun] = relationship(back_populates="task_runs")

    __table_args__ = (
        Index(
            "ix_task_run_unique_attempt",
            "workflow_run_id",
            "task_name",
            "attempt",
            unique=True,
        ),
        Index("ix_task_run_correlation_id", "correlation_id"),
    )
