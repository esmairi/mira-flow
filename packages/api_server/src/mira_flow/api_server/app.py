"""FastAPI app factory — workflow/run/task views + trigger/cancel/retry.

Run with: `uv run uvicorn mira_flow.api_server.app:app`
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from mira_flow.api_server.routers import health, runs, tasks, workflows
from mira_flow.api_server.settings import api_server_settings
from mira_flow.ledger import get_engine
from mira_flow.transport import KafkaBroker
from mira_flow.wiring import load_workflows

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Loads workflow configs and opens the shared broker once at startup.

    Args:
        app: The FastAPI app being started.

    Yields:
        Control back to FastAPI for the app's lifetime; cleans up the
        broker on shutdown.
    """
    # Force the shared ledger Engine/sessionmaker to build now, on this one
    # startup coroutine, before uvicorn starts serving requests —
    # get_engine()'s lazy init has no locking (see ledger/db.py), so
    # without this, the first two concurrent requests' `get_session()`
    # dependencies could race to build it.
    get_engine()
    app.state.workflow_configs = load_workflows(api_server_settings.workflows_dir)
    app.state.broker = KafkaBroker()
    logger.info(
        "api_server: loaded %d workflow(s) from %r",
        len(app.state.workflow_configs),
        api_server_settings.workflows_dir,
    )
    try:
        yield
    finally:
        app.state.broker.close()


def create_app() -> FastAPI:
    """Builds the api_server FastAPI app.

    No auth, permissive CORS (v1, meant for local/trusted-network use — see
    README before deploying this anywhere less trusted).

    Returns:
        The configured app.
    """
    app = FastAPI(title="Mira Flow API server", lifespan=_lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(health.router)
    app.include_router(workflows.router)
    app.include_router(runs.router)
    app.include_router(tasks.router)
    return app


app = create_app()
