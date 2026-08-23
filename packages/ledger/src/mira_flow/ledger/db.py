"""Engine/session setup. URL defaults to settings.database_url."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from mira_flow.ledger.models import Base

# Backend name (SQLAlchemy URL's dialect, e.g. "postgresql") -> the
# packages/ledger extra that installs its driver — see pyproject.toml's
# `optional-dependencies`. Update this alongside adding a new extra there;
# a backend missing here (sqlite included — its driver is stdlib) just
# means `_missing_driver_error` can't suggest a fix, not that it can't run.
_DRIVER_EXTRAS = {
    "postgresql": "postgres",
}


class _LedgerSettings(BaseSettings):
    """`database_url` is a ledger-only concern — kept here rather than in
    transport.settings.Settings, which the rest of the app (including
    ledger-free `core`) shouldn't need to know includes a database at all."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite:///mira_flow.db"


settings = _LedgerSettings()

_engine: Engine | None = None
_Session: sessionmaker[Session] | None = None


def _engine_kwargs(url: str) -> dict[str, Any]:
    """Per-dialect create_engine() tuning, keyed by URL scheme.

    One place to add a new database's quirks (a driver-specific connect_args,
    a pool setting) without touching callers — SQLAlchemy's dialect system
    already means the rest of this module (and every ledger query) stays the
    same across databases, so this dispatch is the only spot that should ever
    need a new branch when a new database is added (see packages/ledger's
    `postgres` extra / pyproject.toml `optional-dependencies` for the driver
    side of the same pattern).

    Args:
        url: The database URL whose scheme selects the dialect.

    Returns:
        Extra kwargs to pass to `create_engine()`.
    """
    if url.startswith("sqlite"):
        # SQLite connections aren't safe to share across threads by default;
        # sessions here are already scoped per-request/per-task, not shared.
        return {"connect_args": {"check_same_thread": False}}
    # Postgres (and most server-based dialects) can silently drop idle
    # connections out from under a pooled engine; pre_ping does one cheap
    # SELECT 1 before reusing a pooled connection so that shows up as a
    # clean reconnect instead of a query failing mid-transaction.
    return {"pool_pre_ping": True}


def _missing_driver_error(url: str, cause: ModuleNotFoundError) -> ModuleNotFoundError:
    """Builds a fail-fast error naming the fix for a missing DBAPI driver.

    `create_engine()` imports the URL's DBAPI driver module immediately
    (constructing the dialect, not lazily at first connection) — so this
    only replaces the resulting raw `ModuleNotFoundError` (which names
    whatever internal driver module SQLAlchemy tried to import, e.g.
    `psycopg2`) with one naming the actual fix. The password is redacted:
    this error is likely to end up in logs.

    Args:
        url: The database URL that failed to build an engine.
        cause: The original `ModuleNotFoundError` from `create_engine()`.

    Returns:
        A new `ModuleNotFoundError`, chained onto `cause`.
    """
    backend = make_url(url).get_backend_name()
    extra = _DRIVER_EXTRAS.get(backend)
    fix = f" — install it with `uv sync --extra {extra}`" if extra else ""
    safe_url = make_url(url).render_as_string(hide_password=True)
    return ModuleNotFoundError(
        f"no driver installed for database {safe_url!r} (dialect {backend!r}){fix}: {cause}"
    )


def get_engine(url: str | None = None) -> Engine:
    """Returns the process-wide SQLAlchemy engine, creating it on first call.

    Args:
        url: Database URL; only used the first time this is called (defaults
            to settings.database_url). Ignored on later calls.

    Returns:
        The shared engine.

    Raises:
        ModuleNotFoundError: If `url`'s DBAPI driver isn't installed.
    """
    global _engine, _Session
    if _engine is None:
        resolved = url or settings.database_url
        try:
            _engine = create_engine(resolved, **_engine_kwargs(resolved))
        except ModuleNotFoundError as exc:
            raise _missing_driver_error(resolved, exc) from exc
        _Session = sessionmaker(bind=_engine, expire_on_commit=False)
    return _engine


def init_db(url: str | None = None) -> None:
    """Creates every ledger table that doesn't already exist.

    Quick local/dev bootstrap only — this has no migration history, so it
    can't evolve an existing database's schema, only create missing tables
    from the current model definitions. For anywhere the schema needs to
    change over time (i.e. any real deployment), use the Alembic migrations
    in `packages/ledger/alembic/` instead (`make migrate`, or `uv run
    alembic -c packages/ledger/alembic.ini upgrade head`) — never both
    against the same database, since `create_all()` doesn't know about
    Alembic's migration history table and vice versa.

    Args:
        url: Database URL, forwarded to `get_engine()`.
    """
    engine = get_engine(url)
    Base.metadata.create_all(engine)


@contextmanager
def session_scope() -> Iterator[Session]:
    """Yields a session, committing on success and rolling back on error.

    Yields:
        A SQLAlchemy session, valid for the duration of the `with` block.
    """
    if _Session is None:
        get_engine()
    assert _Session is not None
    session = _Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
