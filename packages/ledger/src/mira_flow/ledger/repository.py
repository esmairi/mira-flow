"""WorkflowRepository — all workflow_run writes lock_version-guarded (§6)."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from mira_flow.ledger.models import TaskRun, WorkflowRun

logger = logging.getLogger(__name__)

_TERMINAL_TASK_STATUSES = ("SUCCEEDED", "FAILED")
_CANCELLABLE_RUN_STATUSES = ("PENDING", "RUNNING")


class ConcurrentUpdateError(Exception):
    """A workflow_run update lost the optimistic-lock race."""


class MalformedMessageError(Exception):
    """A message with no workflow_run_id arrived on a non-entry topic (§4b)."""


class InvalidTransitionError(Exception):
    """A requested status transition isn't valid from a run's current status."""


def _now() -> datetime:
    return datetime.now(UTC)


def _error_payload(exc: BaseException) -> dict[str, Any]:
    return {"type": type(exc).__name__, "message": str(exc)}


class WorkflowRepository:
    """Reads and writes the workflow_run / task_run ledger for one DB session."""

    def __init__(self, session: Session) -> None:
        """Initializes the repository.

        Args:
            session: The SQLAlchemy session every method operates against.
        """
        self._session = session

    # -- trigger path (§4a) --------------------------------------------------

    def get_or_create_workflow_run(
        self,
        *,
        workflow_name: str,
        workflow_version: int,
        idempotency_key: str,
        input: dict[str, Any],
    ) -> WorkflowRun:
        """Returns the existing run for `idempotency_key`, or creates one.

        Safe under concurrent callers: if two callers race to create the
        same idempotency_key, the loser's insert fails on the unique
        constraint and it falls back to reading the winner's row.

        Args:
            workflow_name: The workflow this run belongs to.
            workflow_version: The workflow's config version at trigger time.
            idempotency_key: Caller-supplied key; repeat triggers with the
                same key return the original run instead of creating a new
                one.
            input: The trigger payload.

        Returns:
            The new or pre-existing PENDING run.
        """
        existing = self._session.scalar(
            select(WorkflowRun).where(WorkflowRun.idempotency_key == idempotency_key)
        )
        if existing is not None:
            logger.debug(
                "workflow_run: reusing existing run %s for idempotency_key %r",
                existing.id,
                idempotency_key,
            )
            return existing

        now = _now()
        run = WorkflowRun(
            workflow_name=workflow_name,
            workflow_version=workflow_version,
            idempotency_key=idempotency_key,
            correlation_id=str(uuid.uuid4()),
            status="PENDING",
            input=input,
            created_at=now,
            updated_at=now,
            lock_version=0,
        )
        self._session.add(run)
        try:
            self._session.flush()
        except IntegrityError:
            self._session.rollback()
            existing = self._session.scalar(
                select(WorkflowRun).where(WorkflowRun.idempotency_key == idempotency_key)
            )
            if existing is None:
                raise
            logger.debug(
                "workflow_run: lost the create race for idempotency_key %r, reusing run %s",
                idempotency_key,
                existing.id,
            )
            return existing
        logger.debug("workflow_run: created run %s (workflow=%r)", run.id, workflow_name)
        return run

    # -- consume path (§4b, §5) ----------------------------------------------

    def start_task_run(
        self,
        *,
        workflow_name: str,
        workflow_version: int,
        task_name: str,
        is_entry: bool,
        workflow_run_id: str | None,
        correlation_id: str | None,
        input: dict[str, Any],
        force: bool = False,
    ) -> TaskRun | None:
        """Inserts the next attempt's RUNNING task_run row.

        Args:
            workflow_name: Used only when creating an implicit run.
            workflow_version: Used only when creating an implicit run.
            task_name: The job this attempt belongs to.
            is_entry: Whether this task is the workflow's entry job.
            workflow_run_id: The run ID carried on the message, or None for
                an untracked entry message (a run is then created
                implicitly).
            correlation_id: The correlation ID carried on the message, if any.
            input: This attempt's input payload.
            force: Bypass the redelivery dedup below and revive a FAILED
                workflow_run back to RUNNING — set for an operator-triggered
                retry (Message.force_retry, admin API), never for a normal
                message. Without this, retrying a FAILED task by simply
                republishing its message would be indistinguishable from
                Kafka redelivering an already-processed message, and would
                just be silently skipped.

        Returns:
            The new task_run row, or None if this message is a redelivery
            of an already-terminal task — the caller should skip the
            handler and just commit the offset.
        """
        run = self._resolve_run(
            workflow_run_id,
            task_name=task_name,
            is_entry=is_entry,
            workflow_name=workflow_name,
            workflow_version=workflow_version,
            correlation_id=correlation_id,
            input=input,
            force=force,
        )
        if run.status == "CANCELLED":
            # Unlike the terminal-task dedup below, force doesn't override
            # this: cancel is a stronger, deliberate operator action, not
            # something a retry should be able to silently undo.
            logger.debug("task_run: %s/%s run is CANCELLED, skipping", run.id, task_name)
            return None

        last = self._last_task_run(run.id, task_name)
        if last is not None and last.status in _TERMINAL_TASK_STATUSES and not force:
            logger.debug(
                "task_run: %s/%s already %s, skipping redelivered message",
                run.id,
                task_name,
                last.status,
            )
            return None

        now = _now()
        attempt = 1 if last is None else last.attempt + 1
        task_run = TaskRun(
            workflow_run_id=run.id,
            task_name=task_name,
            attempt=attempt,
            status="RUNNING",
            input=input,
            correlation_id=run.correlation_id,
            scheduled_at=last.next_retry_at if last is not None else None,
            started_at=now,
        )
        self._session.add(task_run)
        self._session.flush()
        logger.debug(
            "task_run: %s/%s attempt %d started (id=%s)", run.id, task_name, attempt, task_run.id
        )
        return task_run

    def _resolve_run(
        self,
        workflow_run_id: str | None,
        *,
        task_name: str,
        is_entry: bool,
        workflow_name: str,
        workflow_version: int,
        correlation_id: str | None,
        input: dict[str, Any],
        force: bool = False,
    ) -> WorkflowRun:
        """Looks up the run a task_run attempt belongs to.

        Creates one implicitly if the message carries no workflow_run_id
        (an untracked entry message), and flips a PENDING run to RUNNING on
        its first task.

        Args:
            workflow_run_id: The run ID carried on the message, if any.
            task_name: Only used for the error message below.
            is_entry: Whether this task is the workflow's entry job.
            workflow_name: Used only when creating an implicit run.
            workflow_version: Used only when creating an implicit run.
            correlation_id: Used only when creating an implicit run.
            input: Used only when creating an implicit run.
            force: Also revive a FAILED run back to RUNNING — see
                `start_task_run`'s `force` for why.

        Returns:
            The resolved, or newly created, run.

        Raises:
            MalformedMessageError: If a non-entry message has no
                workflow_run_id, or references one that doesn't exist.
        """
        if workflow_run_id is None:
            if not is_entry:
                raise MalformedMessageError(
                    f"message for task {task_name!r} has no workflow_run_id "
                    "and did not arrive on the workflow's entry topic"
                )
            return self._create_implicit_run(
                workflow_name=workflow_name,
                workflow_version=workflow_version,
                correlation_id=correlation_id,
                input=input,
            )

        run = self._session.get(WorkflowRun, workflow_run_id)
        if run is None:
            raise MalformedMessageError(
                f"workflow_run {workflow_run_id!r} referenced by message does not exist"
            )
        if run.status == "PENDING" or (force and run.status == "FAILED"):
            logger.debug("workflow_run: %s %s -> RUNNING", run.id, run.status)
            self._update_run_locked(run, status="RUNNING", started_at=run.started_at or _now())
        return run

    def _last_task_run(self, workflow_run_id: str, task_name: str) -> TaskRun | None:
        """Returns the most recent attempt at this task within this run, if any.

        Args:
            workflow_run_id: The run to search within.
            task_name: The job whose attempts to search.

        Returns:
            The highest-`attempt` task_run row, or None on the first attempt.
        """
        return self._session.scalar(
            select(TaskRun)
            .where(TaskRun.workflow_run_id == workflow_run_id, TaskRun.task_name == task_name)
            .order_by(TaskRun.attempt.desc())
            .limit(1)
        )

    def _create_implicit_run(
        self,
        *,
        workflow_name: str,
        workflow_version: int,
        correlation_id: str | None,
        input: dict[str, Any],
    ) -> WorkflowRun:
        """Creates a RUNNING run with no idempotency_key.

        Used for entry messages published by an external service rather
        than through `get_or_create_workflow_run` — there's no
        idempotency_key to dedup on, so every such message starts a new run.

        Args:
            workflow_name: The workflow this run belongs to.
            workflow_version: The workflow's config version.
            correlation_id: Reused if the message carried one, otherwise a
                fresh one is minted.
            input: The entry message's payload.

        Returns:
            The newly created run.
        """
        now = _now()
        run = WorkflowRun(
            workflow_name=workflow_name,
            workflow_version=workflow_version,
            idempotency_key=None,
            correlation_id=correlation_id or str(uuid.uuid4()),
            status="RUNNING",
            input=input,
            created_at=now,
            started_at=now,
            updated_at=now,
            lock_version=0,
        )
        self._session.add(run)
        self._session.flush()
        logger.debug("workflow_run: created implicit run %s (workflow=%r)", run.id, workflow_name)
        return run

    # -- outcomes -------------------------------------------------------------

    def succeed_task_run(
        self, task_run_id: str, output: dict[str, Any] | None, *, terminal: bool
    ) -> None:
        """Marks a task_run SUCCEEDED, and its run too if this was the last task.

        Args:
            task_run_id: The task_run to mark succeeded.
            output: The handler's return value.
            terminal: Whether this was the workflow's last job — if so, the
                parent workflow_run is marked SUCCEEDED too.

        Note:
            If the run was CANCELLED (by an operator) while this attempt
            was in flight, its status is left alone even if `terminal` —
            a straggling worker finishing after cancellation shouldn't
            resurrect the run back to SUCCEEDED. Only the task_run itself
            is still recorded.
        """
        task_run = self._session.get(TaskRun, task_run_id)
        assert task_run is not None
        now = _now()
        task_run.status = "SUCCEEDED"
        task_run.output = output
        task_run.ended_at = now
        self._session.flush()
        logger.debug("task_run: %s SUCCEEDED (terminal=%s)", task_run_id, terminal)

        if terminal:
            run = self._session.get(WorkflowRun, task_run.workflow_run_id)
            assert run is not None
            if run.status == "CANCELLED":
                logger.debug("workflow_run: %s is CANCELLED, not resurrecting to SUCCEEDED", run.id)
                return
            self._update_run_locked(run, status="SUCCEEDED", output=output, ended_at=now)
            logger.debug("workflow_run: %s SUCCEEDED", run.id)

    def retry_task_run(
        self, task_run_id: str, error: BaseException, next_retry_at: datetime
    ) -> None:
        """Marks a task_run UP_FOR_RETRY; its workflow_run stays RUNNING.

        Args:
            task_run_id: The task_run to mark for retry.
            error: The exception the handler raised.
            next_retry_at: Echoed as `scheduled_at` on the follow-up attempt.
        """
        task_run = self._session.get(TaskRun, task_run_id)
        assert task_run is not None
        task_run.status = "UP_FOR_RETRY"
        task_run.error = _error_payload(error)
        task_run.ended_at = _now()
        task_run.next_retry_at = next_retry_at
        self._session.flush()
        logger.debug(
            "task_run: %s UP_FOR_RETRY, next_retry_at=%s", task_run_id, next_retry_at
        )

    def fail_task_run(self, task_run_id: str, error: BaseException) -> None:
        """Marks a task_run FAILED, and its workflow_run FAILED too.

        Args:
            task_run_id: The task_run to mark failed.
            error: The exception (or NoMatchingRouteError) that caused it.

        Note:
            If the run was CANCELLED (by an operator) while this attempt
            was in flight, its status is left alone — a straggling worker
            finishing after cancellation shouldn't resurrect the run back
            to FAILED. Only the task_run itself is still recorded.
        """
        task_run = self._session.get(TaskRun, task_run_id)
        assert task_run is not None
        now = _now()
        payload = _error_payload(error)
        task_run.status = "FAILED"
        task_run.error = payload
        task_run.ended_at = now
        self._session.flush()
        logger.debug("task_run: %s FAILED: %s", task_run_id, payload)

        run = self._session.get(WorkflowRun, task_run.workflow_run_id)
        assert run is not None
        if run.status == "CANCELLED":
            logger.debug("workflow_run: %s is CANCELLED, not resurrecting to FAILED", run.id)
            return
        self._update_run_locked(run, status="FAILED", error=payload, ended_at=now)
        logger.debug("workflow_run: %s FAILED", run.id)

    # -- operator actions (admin API) ------------------------------------------

    def cancel_workflow_run(self, run_id: str) -> WorkflowRun | None:
        """Marks a workflow_run CANCELLED, if it's still PENDING or RUNNING.

        Only marks the ledger — there's no live signal channel to a
        JobWorker that might be mid-attempt, so this can't interrupt one.
        It does have teeth for anything *after* that: `start_task_run`
        refuses to start a new attempt once its run is CANCELLED, even
        with `force=True` (see there).

        Args:
            run_id: The run to cancel.

        Returns:
            The now-CANCELLED run, or None if no run has that ID.

        Raises:
            InvalidTransitionError: If the run exists but isn't PENDING or
                RUNNING (already terminal, or already CANCELLED).
        """
        run = self._session.get(WorkflowRun, run_id)
        if run is None:
            return None
        if run.status not in _CANCELLABLE_RUN_STATUSES:
            raise InvalidTransitionError(
                f"workflow_run {run_id!r} is {run.status}, "
                "can only cancel a PENDING or RUNNING run"
            )
        self._update_run_locked(run, status="CANCELLED", ended_at=_now())
        logger.debug("workflow_run: %s CANCELLED", run.id)
        return run

    # -- optimistic locking ----------------------------------------------------

    def _update_run_locked(self, run: WorkflowRun, **fields: Any) -> None:
        """Applies a lock_version-guarded UPDATE to a workflow_run.

        Args:
            run: The run to update; its in-memory attributes are refreshed
                in place on success.
            **fields: Columns to set, in addition to lock_version/updated_at.

        Raises:
            ConcurrentUpdateError: If `run.lock_version` no longer matches
                the row (another writer updated it first).
        """
        expected = run.lock_version
        fields["lock_version"] = expected + 1
        fields["updated_at"] = _now()
        result = self._session.execute(
            update(WorkflowRun)
            .where(WorkflowRun.id == run.id, WorkflowRun.lock_version == expected)
            .values(**fields)
        )
        if result.rowcount == 0:
            raise ConcurrentUpdateError(
                f"workflow_run {run.id!r} was updated concurrently (expected lock_version {expected})"
            )
        for key, value in fields.items():
            setattr(run, key, value)
        self._session.flush()
        logger.debug(
            "workflow_run: %s lock_version %d -> %d", run.id, expected, expected + 1
        )

    # -- read queries (admin UI) -----------------------------------------------

    def list_workflow_runs(
        self,
        *,
        workflow_name: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[WorkflowRun]:
        """Lists workflow_runs, newest first.

        Args:
            workflow_name: Only runs for this workflow, if set.
            status: Only runs in this status, if set.
            limit: Max rows to return.
            offset: Rows to skip, for pagination.

        Returns:
            Matching runs, newest `created_at` first.
        """
        stmt = select(WorkflowRun)
        if workflow_name is not None:
            stmt = stmt.where(WorkflowRun.workflow_name == workflow_name)
        if status is not None:
            stmt = stmt.where(WorkflowRun.status == status)
        stmt = stmt.order_by(WorkflowRun.created_at.desc()).limit(limit).offset(offset)
        return list(self._session.scalars(stmt))

    def get_workflow_run(self, run_id: str) -> WorkflowRun | None:
        """Gets one workflow_run by ID.

        Args:
            run_id: The run's ID.

        Returns:
            The matching run, or None if no run has that ID.
        """
        return self._session.get(WorkflowRun, run_id)

    def list_task_runs(self, workflow_run_id: str) -> list[TaskRun]:
        """Lists every task_run attempt within one run.

        Args:
            workflow_run_id: The run to list attempts for.

        Returns:
            Matching attempts, ordered by task name then attempt number.
        """
        stmt = (
            select(TaskRun)
            .where(TaskRun.workflow_run_id == workflow_run_id)
            .order_by(TaskRun.task_name, TaskRun.attempt)
        )
        return list(self._session.scalars(stmt))

    def get_task_run(self, task_run_id: str) -> TaskRun | None:
        """Gets one task_run by ID.

        Args:
            task_run_id: The attempt's ID.

        Returns:
            The matching attempt, or None if no attempt has that ID.
        """
        return self._session.get(TaskRun, task_run_id)
