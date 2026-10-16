"""Live Kafka verification. --check is read-only; other modes are explicit.

--provision creates the four business topics. Fault/restart modes affect only
the v2 brokers, and create unique smoke topics retained for inspection.
"""
import argparse
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer, TopicCollection, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'spark'))
from kafka_cluster import TOPICS, bootstrap_servers, ensure_kafka_topics


def docker(*arguments, timeout=45):
    result = subprocess.run(['docker', *arguments], capture_output=True, text=True,
                            encoding='utf-8', errors='replace', timeout=timeout, check=True)
    return result.stdout.strip()


def wait_for(check, description, seconds=60):
    deadline = time.monotonic() + seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except Exception as exc:
            last_error = exc
        time.sleep(1)
    raise TimeoutError(f'{description}: {last_error}')


def cluster_check(bootstrap):
    for endpoint in bootstrap.split(','):
        admin = AdminClient({'bootstrap.servers': endpoint})
        state = admin.list_topics(timeout=8)
        if set(state.brokers) != {1, 2, 3}:
            raise AssertionError(f'{endpoint}: expected broker IDs 1,2,3, got {list(state.brokers)}')
        for topic in TOPICS:
            info = state.topics.get(topic)
            if info is None or info.error:
                raise AssertionError(f'{endpoint}: topic {topic} missing or unreadable')
            if len(info.partitions) != 2 or any(
                    len(p.replicas) != 2 or len(p.isrs) != 2 for p in info.partitions.values()):
                raise AssertionError(f'{topic}: expected 2 partitions, RF=2, ISR=2')
        resources = [ConfigResource(ResourceType.TOPIC, t) for t in TOPICS]
        for resource, future in admin.describe_configs(resources, request_timeout=8).items():
            if future.result(timeout=8)['min.insync.replicas'].value != '2':
                raise AssertionError(f'{resource.name}: min.insync.replicas != 2')
        print(f'PASS metadata/policy via {endpoint}', flush=True)


def send(bootstrap, topic, partition, record_id, expect_success=True):
    errors, acknowledged = [], []
    producer = Producer({'bootstrap.servers': bootstrap, 'acks': 'all', 'retries': 3 if expect_success else 0,
                         'delivery.timeout.ms': 10_000, 'request.timeout.ms': 3000,
                         'socket.timeout.ms': 3000})
    def callback(error, message):
        (errors if error else acknowledged).append(error or (message.partition(), message.offset()))
    producer.produce(topic, partition=partition, key=record_id.encode(),
                     value=json.dumps({'id': record_id}).encode(), callback=callback)
    pending = producer.flush(15)
    if pending:
        raise AssertionError('Delivery callback did not complete')
    if expect_success and (errors or len(acknowledged) != 1):
        raise AssertionError(f'Expected acknowledgment: {errors}')
    if not expect_success and (acknowledged or len(errors) != 1):
        raise AssertionError('Write was acknowledged despite ISR below 2')
    if not expect_success and (not isinstance(errors[0], KafkaError) or errors[0].code() not in
                               (KafkaError.NOT_ENOUGH_REPLICAS, KafkaError.NOT_ENOUGH_REPLICAS_AFTER_APPEND)):
        raise AssertionError(f'Failure is not evidence of min ISR rejection: {errors[0]}')
    return acknowledged


def read_ids(bootstrap, topic, expected, seconds=30):
    consumer = Consumer({'bootstrap.servers': bootstrap, 'group.id': 'cluster-smoke-' + uuid.uuid4().hex,
                         'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
    found = set()
    try:
        consumer.assign([TopicPartition(topic, p, 0) for p in range(2)])
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline and not expected.issubset(found):
            message = consumer.poll(1)
            if message is not None and message.error():
                raise RuntimeError(message.error())
            if message is not None:
                found.add(json.loads(message.value())['id'])
    finally:
        consumer.close()
    if not expected.issubset(found):
        raise AssertionError(f'Missing acknowledged records: {expected - found}')


def topic_identity(admin, topic):
    info = admin.describe_topics(TopicCollection([topic]), request_timeout=8)[topic].result(timeout=8)
    return str(info.topic_id), tuple((p.id, tuple(b.id for b in p.replicas)) for p in info.partitions)


def recovery_test(bootstrap, fault=False, restart=False):
    topic = 'cluster_smoke_' + uuid.uuid4().hex
    admin = AdminClient({'bootstrap.servers': bootstrap})
    admin.create_topics([NewTopic(topic, num_partitions=2, replica_assignment=[[1, 2], [2, 3]],
                         config={'min.insync.replicas': '2'})], request_timeout=10)[topic].result(timeout=10)
    ensure_kafka_topics(bootstrap, [topic])
    def partitions():
        return admin.list_topics(timeout=8).topics[topic].partitions
    wait_for(lambda: all(len(p.isrs) == 2 for p in partitions().values()), 'initial ISR')
    expected = {f'{topic}-before-{p}' for p in range(2)}
    for p in range(2):
        send(bootstrap, topic, p, f'{topic}-before-{p}')
    read_ids(bootstrap, topic, expected)
    print(f'PASS both partitions acknowledged and consumed: {topic}', flush=True)
    if fault:
        broker_id = 1  # deterministically exercise loss of the first bootstrap endpoint
        broker = f'kafka-{broker_id}'
        survivor = next(f'kafka-{i}' for i in (1, 2, 3) if i != broker_id)
        try:
            docker('stop', '-t', '10', broker)
            wait_for(lambda: len(partitions()[0].isrs) < 2, 'ISR drops below min ISR')
            wait_for(lambda: partitions()[0].leader in (2, 3), 'surviving partition leader elected')
            wait_for(lambda: 'LeaderId:' in docker('exec', survivor, '/opt/kafka/bin/kafka-metadata-quorum.sh',
                     '--bootstrap-server', f'{survivor}:9092', 'describe', '--status'), 'metadata quorum leader')
            send(bootstrap, topic, 0, topic + '-uncertain', expect_success=False)
            AdminClient({'bootstrap.servers': bootstrap}).list_topics(timeout=8)
            internal = docker('exec', survivor, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server',
                              'kafka-1:9092,kafka-2:9092,kafka-3:9092', '--describe', '--topic', topic)
            if topic not in internal:
                raise AssertionError('INTERNAL bootstrap list failed with kafka-1 down')
            for i, port in ((1, 9093), (2, 19093), (3, 29093)):
                if i != broker_id:
                    AdminClient({'bootstrap.servers': f'127.0.0.1:{port}'}).list_topics(timeout=8)
            print('PASS quorum available; acks=all write rejected below min ISR', flush=True)
        finally:
            docker('start', broker)
        wait_for(lambda: all(len(p.isrs) == 2 for p in partitions().values()), 'ISR recovered')
        send(bootstrap, topic, 0, topic + '-after')
        expected.add(topic + '-after')
        read_ids(bootstrap, topic, expected)
        print('PASS broker recovery and new write; uncertain write excluded from success claims', flush=True)
    if restart:
        before_cluster = admin.list_topics(timeout=8).cluster_id
        before_topic = topic_identity(admin, topic)
        try:
            docker('restart', 'kafka-1', 'kafka-2', 'kafka-3', timeout=60)
        finally:
            for name in ('kafka-1', 'kafka-2', 'kafka-3'):
                docker('start', name)
        wait_for(lambda: all(len(p.isrs) == 2 for p in partitions().values()), 'cluster restart ISR', seconds=120)
        if admin.list_topics(timeout=8).cluster_id != before_cluster or topic_identity(admin, topic) != before_topic:
            raise AssertionError('Cluster/topic ID or replica assignments changed across restart')
        read_ids(bootstrap, topic, expected)
        print('PASS restart preserved cluster/topic IDs, assignments and acknowledged records', flush=True)


def reject_wrong_policy(bootstrap):
    admin = AdminClient({'bootstrap.servers': bootstrap})
    for partitions, replicas, min_isr in ((1, 1, '1'), (2, 2, '1')):
        topic = 'cluster_smoke_bad_' + uuid.uuid4().hex
        admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replicas,
                             config={'min.insync.replicas': min_isr})], request_timeout=10)[topic].result(timeout=10)
        try:
            ensure_kafka_topics(bootstrap, [topic])
        except ValueError:
            print(f'PASS refused incompatible topic without altering it: {topic}', flush=True)
        else:
            raise AssertionError(f'Incompatible topic was accepted: {topic}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Read-only metadata/policy checks (default)')
    parser.add_argument('--provision', action='store_true', help='Create/validate the four business topics')
    parser.add_argument('--fault-test', action='store_true', help='Stop and restore one v2 broker')
    parser.add_argument('--restart-test', action='store_true', help='Restart all three v2 brokers')
    parser.add_argument('--policy-test', action='store_true', help='Create incompatible test topics and verify rejection')
    args = parser.parse_args()
    load_dotenv(ROOT / '.env')
    bootstrap = bootstrap_servers()
    if args.provision:
        ensure_kafka_topics(bootstrap, list(TOPICS))
    cluster_check(bootstrap)
    if args.policy_test:
        reject_wrong_policy(bootstrap)
    if args.fault_test or args.restart_test:
        recovery_test(bootstrap, args.fault_test, args.restart_test)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'FAIL: {exc}', file=sys.stderr)
        sys.exit(1)
