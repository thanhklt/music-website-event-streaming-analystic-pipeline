"""Offline regression tests for policy validation and delivery failures."""
import importlib.util
import sys
import tempfile
import unittest
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'spark'))
sys.path.insert(0, str(ROOT / 'scripts'))
from kafka_cluster import bootstrap_servers, ensure_kafka_topics


def done(value):
    future = Future()
    future.set_result(value)
    return future


def metadata(partitions=2, replicas=(1, 2), error=None):
    return SimpleNamespace(topics={'events': SimpleNamespace(error=error, partitions={
        p: SimpleNamespace(replicas=list(replicas), isrs=list(replicas), error=None)
        for p in range(partitions)
    })})


class KafkaPolicyTests(unittest.TestCase):
    def admin(self, state):
        admin = Mock()
        admin.list_topics.return_value = state
        admin.describe_configs.side_effect = lambda resources, **kw: {
            r: done({'min.insync.replicas': SimpleNamespace(value='2')}) for r in resources
        }
        return admin

    def test_bootstrap_prefers_list_and_retains_legacy_fallback(self):
        self.assertEqual(bootstrap_servers({'KAFKA_BOOTSTRAP_SERVERS': 'a:1,b:2'}), 'a:1,b:2')
        self.assertEqual(bootstrap_servers({'KAFKA_ADDRESS': 'old', 'KAFKA_PORT': '123'}), 'old:123')

    def test_creates_missing_topic_with_two_partitions_two_replicas_and_min_isr(self):
        admin = self.admin(metadata())
        admin.list_topics.side_effect = [SimpleNamespace(topics={}), metadata()]
        admin.create_topics.return_value = {'events': done(None)}
        with patch('kafka_cluster.AdminClient', return_value=admin):
            ensure_kafka_topics('a:1,b:2', ['events'])
        topic = admin.create_topics.call_args.args[0][0]
        self.assertEqual((topic.num_partitions, topic.replication_factor), (2, 2))
        self.assertEqual(topic.config, {'min.insync.replicas': '2'})

    def test_existing_correct_topic_is_not_recreated(self):
        admin = self.admin(metadata())
        with patch('kafka_cluster.AdminClient', return_value=admin):
            ensure_kafka_topics('a:1,b:2', ['events'])
        admin.create_topics.assert_not_called()

    def test_existing_wrong_partition_or_replica_count_is_rejected(self):
        for state in [metadata(partitions=1), metadata(replicas=(1,))]:
            with self.subTest(state=state), patch('kafka_cluster.AdminClient', return_value=self.admin(state)):
                with self.assertRaises(ValueError):
                    ensure_kafka_topics('a:1,b:2', ['events'])

    def test_effective_min_isr_is_checked(self):
        admin = self.admin(metadata())
        admin.describe_configs.side_effect = lambda resources, **kw: {
            r: done({'min.insync.replicas': SimpleNamespace(value='1')}) for r in resources
        }
        with patch('kafka_cluster.AdminClient', return_value=admin):
            with self.assertRaises(ValueError):
                ensure_kafka_topics('a:1,b:2', ['events'])

    def test_topic_creation_race_is_revalidated(self):
        from confluent_kafka import KafkaError, KafkaException
        admin = self.admin(metadata())
        admin.list_topics.side_effect = [SimpleNamespace(topics={}), metadata()]
        future = Future()
        future.set_exception(KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS)))
        admin.create_topics.return_value = {'events': future}
        with patch('kafka_cluster.AdminClient', return_value=admin):
            ensure_kafka_topics('a:1,b:2', ['events'])
        self.assertEqual(admin.list_topics.call_count, 2)

    def test_unavailable_cluster_has_bounded_deadline(self):
        admin = self.admin(metadata())
        admin.list_topics.side_effect = RuntimeError('unavailable')
        with patch('kafka_cluster.AdminClient', return_value=admin):
            with self.assertRaises(TimeoutError):
                ensure_kafka_topics('a:1', ['events'], timeout_seconds=0.01)


class ProducerDeliveryTests(unittest.TestCase):
    def test_fault_probe_rejects_transport_errors_as_min_isr_evidence(self):
        import test_kafka_cluster as smoke
        producer = Mock()
        producer.produce.side_effect = lambda *args, **kw: kw['callback'](RuntimeError('transport unavailable'), Mock())
        producer.flush.return_value = 0
        with patch.object(smoke, 'Producer', return_value=producer):
            with self.assertRaises(AssertionError):
                smoke.send('a:1,b:2', 'events', 0, 'record', expect_success=False)

    def test_cleanup_attempts_every_container_even_if_log_or_stop_fails(self):
        import pipeline_smoke as smoke
        with tempfile.TemporaryDirectory() as directory:
            calls = []
            def docker(*args, **kw):
                calls.append(args)
                if args[0] == 'logs' or args[-1] == 'nn':
                    raise OSError('unavailable')
                return ''
            with patch.object(smoke, 'docker', side_effect=docker):
                smoke.cleanup_smoke(Path(directory), 'spark', ('net', 'nn', 'dn'))
            self.assertIn(('stop', '-t', '15', 'spark'), calls)
            self.assertIn(('stop', '-t', '15', 'nn'), calls)
            self.assertIn(('stop', '-t', '15', 'dn'), calls)

    def test_callback_failure_or_unflushed_messages_cannot_report_success(self):
        spec = importlib.util.spec_from_file_location('sample_producer', ROOT / 'kafka/test_producer.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for failure, pending in [(True, 0), (False, 1)]:
            with self.subTest(failure=failure, pending=pending), tempfile.TemporaryDirectory() as directory:
                Path(directory, 'sample.jsonl').write_text('{"userId": 1}\n', encoding='utf-8')
                producer = Mock()
                def produce(**kwargs):
                    kwargs['callback'](RuntimeError('not enough replicas') if failure else None, Mock())
                producer.produce.side_effect = produce
                producer.flush.return_value = pending
                with self.assertRaises(RuntimeError):
                    module.load_and_produce(producer, 'events', Path(directory))


if __name__ == '__main__':
    unittest.main()
