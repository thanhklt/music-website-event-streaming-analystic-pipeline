"""Kafka bootstrap and topic policy, importable without starting Spark."""
import os
import time
from collections.abc import Mapping

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

HOST_BOOTSTRAP = '127.0.0.1:9093,127.0.0.1:19093,127.0.0.1:29093'
INTERNAL_BOOTSTRAP = 'kafka-1:9092,kafka-2:9092,kafka-3:9092'
TOPICS = ('auth_events', 'listen_events', 'page_view_events', 'status_change_events')
PARTITIONS = REPLICATION_FACTOR = MIN_ISR = 2


def bootstrap_servers(environ: Mapping[str, str] | None = None, default: str = HOST_BOOTSTRAP) -> str:
    env = os.environ if environ is None else environ
    if env.get('KAFKA_BOOTSTRAP_SERVERS', '').strip():
        return env['KAFKA_BOOTSTRAP_SERVERS'].strip()
    if env.get('KAFKA_ADDRESS') or env.get('KAFKA_PORT'):
        return f"{env.get('KAFKA_ADDRESS', 'localhost')}:{env.get('KAFKA_PORT', '9093')}"
    return default


def ensure_kafka_topics(bootstrap: str, topics: list, timeout_seconds: float = 60) -> None:
    """Create missing topics; refuse incompatible existing topics without altering them.

    All RPCs and futures share one deadline. Transient metadata/creation failures
    retry; an existing topic with the wrong policy is a configuration error.
    """
    admin = AdminClient({'bootstrap.servers': bootstrap})
    deadline = time.monotonic() + timeout_seconds

    def remaining():
        value = deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError(f'Kafka topic provisioning exceeded {timeout_seconds}s')
        return min(value, 10)

    last_error = None
    while time.monotonic() < deadline:
        try:
            state = admin.list_topics(timeout=remaining())
            missing = [t for t in topics if t not in state.topics
                       or (state.topics[t].error is not None
                           and state.topics[t].error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART)]
            if missing:
                new = [NewTopic(t, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR,
                                config={'min.insync.replicas': str(MIN_ISR)}) for t in missing]
                futures = admin.create_topics(new, request_timeout=remaining())
                for future in futures.values():
                    try:
                        future.result(timeout=remaining())
                    except KafkaException as exc:
                        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                            raise
                state = admin.list_topics(timeout=remaining())
            for topic in topics:
                info = state.topics.get(topic)
                if info is None:
                    raise RuntimeError(f'Topic {topic} not yet visible')
                if info.error:
                    raise KafkaException(info.error)
                if len(info.partitions) != PARTITIONS or any(
                        len(p.replicas) != REPLICATION_FACTOR for p in info.partitions.values()):
                    raise ValueError(f'{topic}: requires {PARTITIONS} partitions and RF={REPLICATION_FACTOR}; '
                                     'existing topic was not changed')
            resources = [ConfigResource(ResourceType.TOPIC, t) for t in topics]
            configs = admin.describe_configs(resources, request_timeout=remaining())
            for resource, future in configs.items():
                config = future.result(timeout=remaining())
                if config['min.insync.replicas'].value != str(MIN_ISR):
                    raise ValueError(f'{resource.name}: requires min.insync.replicas={MIN_ISR}; '
                                     'existing topic was not changed')
            return
        except ValueError:
            raise
        except (KafkaException, RuntimeError, TimeoutError) as exc:
            last_error = exc
            time.sleep(max(0, min(1, deadline - time.monotonic())))
    raise TimeoutError(f'Kafka topic provisioning exceeded {timeout_seconds}s: {last_error}') from last_error
