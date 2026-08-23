"""Core — run a single workflow end-to-end without the DB ledger.

Loads one config, builds a dedicated consumer (core.consumer) + producer
(core.producer) per job, runs one JobWorker per (job, consumer instance),
all orchestrated by a WorkflowManager. No workflow_run / task_run history —
see `job_worker.py`'s module docstring for exactly what that gives up.
Reach for `orchestrator` instead when you need the run ledger, crash-safe
retry resume, or multi-workflow serving.

JobWorker implements execution.Worker — the shared shape both `core` and
`orchestrator` execution units satisfy, even though their internals differ.

Also exports `JobBinding`/`build_bindings` (config x registry -> runnable
unit): a pure, DB-free construction step, so `orchestrator` (which does need
the ledger) builds its bindings via `core` rather than duplicating that
logic.
"""

from mira_flow.core.bindings import JobBinding, build_bindings
from mira_flow.core.consumer import ConsumerFactory, TopicConsumer
from mira_flow.core.errors import CoreBrokerError
from mira_flow.core.job_worker import JobWorker
from mira_flow.core.manager import WorkflowManager
from mira_flow.core.producer import ProducerFactory, TopicProducer

__all__ = [
    "ConsumerFactory",
    "CoreBrokerError",
    "JobBinding",
    "JobWorker",
    "ProducerFactory",
    "TopicConsumer",
    "TopicProducer",
    "WorkflowManager",
    "build_bindings",
]
