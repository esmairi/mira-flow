"""WorkflowManager — runs a single workflow config end-to-end, no ledger."""

from __future__ import annotations

import logging
import threading
from pathlib import Path

from mira_flow.core.consumer import ConsumerFactory
from mira_flow.core.job_worker import JobWorker
from mira_flow.core.producer import ProducerFactory, TopicProducer
from mira_flow.execution import ResolvedRoute, Worker
from mira_flow.transport.message import Message
from mira_flow.wiring import (
    ConsumerSpec,
    HandlerRegistry,
    JobSpec,
    ProducerSpec,
    WorkflowConfig,
    default_registry,
    load_avro_schema,
)
from mira_flow.wiring.config import parse_workflow_file

logger = logging.getLogger(__name__)


class WorkflowManager:
    """Loads one WorkflowConfig, builds one JobWorker per job (or several, if
    the job's ConsumerSpec.instances > 1 — §2), runs them.

    No workflow_run / task_run rows: `trigger` just publishes to the entry
    consumer's topic, each JobWorker consumes/handles/produces on its own,
    nothing is recorded.
    """

    def __init__(
        self,
        config: WorkflowConfig,
        registry: HandlerRegistry | None = None,
        bootstrap_servers: str | None = None,
        base_dir: Path | None = None,
    ) -> None:
        """Initializes the manager and builds every job's workers.

        Args:
            config: The workflow to run.
            registry: Handler lookup; defaults to the global registry.
            bootstrap_servers: Kafka bootstrap servers; defaults to
                settings.kafka_bootstrap_servers.
            base_dir: Directory a consumer/producer's schema_avro_value is
                resolved relative to (the config file's own directory) —
                needed to actually load and validate against those schemas
                at runtime. None skips runtime Avro validation entirely
                (schemas, if any, were still shape-validated at parse time).
        """
        self._config = config
        registry = registry or default_registry()
        consumer_by_name = {c.name: c for c in config.consumers}
        producer_by_name = {p.name: p for p in config.producers}
        consumer_factory = ConsumerFactory(bootstrap_servers)
        producer_factory = ProducerFactory(bootstrap_servers)

        self._workers: list[Worker] = []
        self._worker_labels: list[str] = []
        for job in config.jobs:
            producers, output_routes = self._build_producers(
                job, producer_by_name, producer_factory, base_dir
            )
            workers, labels = self._build_workers(
                job,
                consumer_by_name[job.consumer],
                producers,
                output_routes,
                consumer_factory,
                registry,
                base_dir,
            )
            self._workers.extend(workers)
            self._worker_labels.extend(labels)
            logger.debug(
                "job %r: %d worker instance(s), %d output route(s)",
                job.name,
                len(workers),
                len(output_routes),
            )

        self._entry_topic, self._entry_producer = self._resolve_entry(
            config, consumer_by_name, producer_factory, base_dir
        )
        self._threads: list[threading.Thread] = []
        logger.info(
            "workflow %r: built %d worker(s) across %d job(s)",
            config.name,
            len(self._workers),
            len(config.jobs),
        )

    @staticmethod
    def _build_producers(
        job: JobSpec,
        producer_by_name: dict[str, ProducerSpec],
        producer_factory: ProducerFactory,
        base_dir: Path | None,
    ) -> tuple[dict[str, TopicProducer], tuple[ResolvedRoute, ...]]:
        """Builds one TopicProducer per unique destination topic.

        librdkafka's Producer is safe for concurrent produce() calls, so one
        is shared across all of the job's consumer instances.

        Args:
            job: The job whose producer routes to build.
            producer_by_name: All configured producers, keyed by name.
            producer_factory: Builds one TopicProducer per topic.
            base_dir: See `__init__`.

        Returns:
            A (topic -> TopicProducer, resolved routes for route selection)
            pair.
        """
        producers: dict[str, TopicProducer] = {}
        routes: list[ResolvedRoute] = []
        for route in job.producer:
            spec = producer_by_name[route.name]
            if spec.topic not in producers:
                schema = (
                    load_avro_schema(spec.schema_avro_value, base_dir)
                    if base_dir is not None
                    else None
                )
                producers[spec.topic] = producer_factory.create(
                    spec.topic, params=spec.params, avro_schema=schema, name=job.name
                )
            routes.append(ResolvedRoute(topic=spec.topic, when=route.when))
        return producers, tuple(routes)

    @staticmethod
    def _build_workers(
        job: JobSpec,
        consumer_spec: ConsumerSpec,
        producers: dict[str, TopicProducer],
        output_routes: tuple[ResolvedRoute, ...],
        consumer_factory: ConsumerFactory,
        registry: HandlerRegistry,
        base_dir: Path | None,
    ) -> tuple[list[Worker], list[str]]:
        """Builds one JobWorker per consumer instance (§2 ConsumerSpec.instances).

        Each instance gets its own Consumer client, all in the same group so
        Kafka splits the topic's partitions across them. `core` has no
        ledger to race on, so instances need no coordination of their own —
        Kafka's per-key partition affinity (Message.kafka_key defaults to
        workflow_run_id) just means messages for one run still arrive in
        order, if a handler cares.

        Args:
            job: The job to build workers for.
            consumer_spec: The job's input topic config, including instances.
            producers: This job's topic -> TopicProducer map.
            output_routes: This job's resolved producer routes.
            consumer_factory: Builds one TopicConsumer per instance.
            registry: Resolves the job's handler by name.
            base_dir: See `__init__`.

        Returns:
            A (workers, matching labels for logging/thread names) pair.
        """
        schema = (
            load_avro_schema(consumer_spec.schema_avro_value, base_dir)
            if base_dir is not None
            else None
        )
        labels = [f"{job.name}#{i}" for i in range(consumer_spec.instances)]
        workers: list[Worker] = [
            JobWorker(
                task_name=job.name,
                handler=registry.resolve(job.handler),
                params=job.params,
                consumer=consumer_factory.create(
                    consumer_spec.topic,
                    consumer_spec.group,
                    params=consumer_spec.params,
                    avro_schema=schema,
                    name=label,
                ),
                producers=producers,
                output_routes=output_routes,
                retries=job.retries,
                retry_delay_seconds=job.retry_delay_seconds,
            )
            for label in labels
        ]
        return workers, labels

    @staticmethod
    def _resolve_entry(
        config: WorkflowConfig,
        consumer_by_name: dict[str, ConsumerSpec],
        producer_factory: ProducerFactory,
        base_dir: Path | None,
    ) -> tuple[str | None, TopicProducer | None]:
        """Resolves entry_consumer to its topic and a TopicProducer for `trigger()`.

        Reuses a declared producer's params if one happens to target the
        same topic, best-effort — entry_consumer names a consumer, so
        there's no direct producer identity to look up.

        Args:
            config: The workflow whose entry_consumer to resolve.
            consumer_by_name: All configured consumers, keyed by name.
            producer_factory: Builds the entry TopicProducer.
            base_dir: See `__init__`.

        Returns:
            An (entry topic, entry producer) pair, or (None, None) if the
            workflow has no entry_consumer.
        """
        if config.entry_consumer is None:
            return None, None
        entry_topic = consumer_by_name[config.entry_consumer].topic
        matching_spec = next((p for p in config.producers if p.topic == entry_topic), None)
        schema = (
            load_avro_schema(matching_spec.schema_avro_value, base_dir)
            if base_dir is not None and matching_spec is not None
            else None
        )
        entry_producer = producer_factory.create(
            entry_topic,
            params=matching_spec.params if matching_spec else None,
            avro_schema=schema,
            name=f"{config.name}.trigger",
        )
        return entry_topic, entry_producer

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        registry: HandlerRegistry | None = None,
        bootstrap_servers: str | None = None,
    ) -> WorkflowManager:
        """Builds a manager from a single workflow config file.

        Args:
            path: Path to the workflow's JSON config.
            registry: Handler lookup; defaults to the global registry.
            bootstrap_servers: Kafka bootstrap servers; defaults to
                settings.kafka_bootstrap_servers.

        Returns:
            The constructed manager, ready to `trigger()`/`run_forever()`.
        """
        path = Path(path)
        config = parse_workflow_file(path)
        return cls(
            config, registry=registry, bootstrap_servers=bootstrap_servers, base_dir=path.parent
        )

    def trigger(self, payload: dict) -> None:
        """Publishes a message to the workflow's entry consumer topic.

        Args:
            payload: The JSON-serializable message payload.

        Raises:
            ValueError: If the workflow has no entry_consumer.
        """
        if self._entry_producer is None:
            raise ValueError(
                f"workflow {self._config.name!r} has no entry_consumer; "
                "it must be started by publishing directly to its first job's topic"
            )
        # _resolve_entry sets topic/producer together — producer is only
        # non-None when topic is too.
        assert self._entry_topic is not None
        self._entry_producer.publish(Message(topic=self._entry_topic, payload=payload))

    def run_forever(self) -> None:
        """Starts one thread per worker and blocks until they all exit.

        Ctrl+C (KeyboardInterrupt) triggers a clean `stop()`.
        """
        logger.info("workflow %r: starting %d worker(s)", self._config.name, len(self._workers))
        self._threads = [
            threading.Thread(
                target=worker.run,
                name=f"core:{self._config.name}.{label}",
                daemon=True,
            )
            for worker, label in zip(self._workers, self._worker_labels, strict=True)
        ]
        for thread in self._threads:
            thread.start()
        try:
            while any(t.is_alive() for t in self._threads):
                for t in self._threads:
                    t.join(timeout=0.5)
        except KeyboardInterrupt:
            self.stop()
        logger.info("workflow %r: all workers stopped", self._config.name)

    def stop(self) -> None:
        """Signals every worker to stop and waits for their threads to exit."""
        logger.info("workflow %r: stopping", self._config.name)
        for worker in self._workers:
            worker.stop()
        for thread in self._threads:
            thread.join(timeout=5)
