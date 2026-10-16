"""Isolated sample → Kafka → Spark → HDFS → DuckDB → dbt → dashboard verification.

Creates unique topics, HDFS paths, a Spark container and a test warehouse. Never
uses the existing warehouse/checkpoints; retains artifacts, stops its own Spark.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from xml.sax.saxutils import escape
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'spark'))
from kafka_cluster import bootstrap_servers, ensure_kafka_topics
sys.path.insert(0, str(ROOT / 'spark/batch'))
from hdfs_to_duckdb import run_ingestion, _fetch_webhdfs_bytes
from test_kafka_cluster import docker, wait_for


def samples():
    """Two independent users; each has two plays ending at a later page event.

    2 auth, 4 listen, 2 page, 2 status records. Each user's listened seconds:
    first play 120s, second play 60s; total 4 plays / 2 users / 360 seconds.
    """
    result = {t: [] for t in ('auth_events', 'listen_events', 'page_view_events', 'status_change_events')}
    start = int(datetime(2026, 10, 16, 8, tzinfo=timezone.utc).timestamp() * 1000)
    for user, gender in ((900001, 'F'), (900002, 'M')):
        base = dict(ts=start, sessionId=user, userId=user, level='paid', itemInSession=0,
                    city='San Francisco', zip='94105', state='CA', userAgent='pipeline-smoke',
                    lon=-122.4, lat=37.8, lastName='Fixture', firstName='Sample', gender=gender,
                    registration=start - 86400000, auth='Logged In')
        result['auth_events'].append(dict(base, ts=start - 1000, success=True))
        result['status_change_events'].append(dict(base, ts=start - 500))
        result['listen_events'].append(dict(base, itemInSession=1, artist='Fixture Artist',
                                            song='Fixture A', duration=200.0))
        result['listen_events'].append(dict(base, ts=start + 120000, itemInSession=2,
                                            artist='Fixture Artist', song='Fixture B', duration=180.0))
        result['page_view_events'].append(dict(base, ts=start + 180000, itemInSession=3,
                                               page='Home', method='GET', status=200))
    return result


def produce(bootstrap, topic, records):
    producer = Producer({'bootstrap.servers': bootstrap, 'acks': 'all', 'retries': 3,
                         'delivery.timeout.ms': 30000})
    acknowledged, errors = [], []
    def callback(error, message):
        if error:
            errors.append(str(error))
        else:
            acknowledged.append((message.partition(), message.offset()))
    for index, record in enumerate(records):
        producer.produce(topic, partition=index % 2, key=str(record['userId']).encode(),
                         value=json.dumps(record).encode(), callback=callback)
        producer.poll(0)
    if producer.flush(35) or errors or len(acknowledged) != len(records):
        raise AssertionError(f'Producer delivery failure: {errors}')
    return set(acknowledged)


def committed_records(webhdfs, output, topic):
    """Read only files listed by Spark commit logs, never a directory snapshot."""
    import pyarrow.parquet as pq
    import io
    response = requests.get(f'{webhdfs}{output}/{topic}/_spark_metadata?op=LISTSTATUS', timeout=5)
    if response.status_code == 404:
        return []
    response.raise_for_status()
    files = set()
    for item in response.json()['FileStatuses']['FileStatus']:
        name = item['pathSuffix']
        if not (name.isdigit() or (name.endswith('.compact') and name[:-8].isdigit())):
            continue
        text = _fetch_webhdfs_bytes(f'{webhdfs}{output}/{topic}/_spark_metadata/{name}?op=OPEN').decode()
        for line in text.splitlines()[1:]:
            entry = json.loads(line)
            if entry.get('action') == 'add':
                from urllib.parse import urlparse
                files.add(urlparse(entry['path']).path)
    records = []
    for path in sorted(files):
        raw = _fetch_webhdfs_bytes(f'{webhdfs}{path}?op=OPEN')
        records.extend(pq.ParquetFile(io.BytesIO(raw)).read().to_pylist())
    return records


def verify_offsets(webhdfs, output, expected):
    all_rows = {}
    for topic, offsets in expected.items():
        rows = committed_records(webhdfs, output, topic)
        actual = [(row['partition'], row['offset']) for row in rows]
        if set(actual) != offsets or len(actual) != len(offsets):
            return False
        all_rows[topic] = rows
    return all_rows


def downstream(work, output, webhdfs, counts):
    database = work / 'smoke.duckdb'
    with duckdb.connect(str(database)) as con:
        con.execute((ROOT / 'infra/duckdb/init.sql').read_text(encoding='utf-8'))
    first = run_ingestion(duckdb_path=str(database), hdfs_base_path=output,
                          webhdfs_url=webhdfs, staging_dir=str(work / 'staging'))
    if first != counts:
        raise AssertionError(f'Landing counts differ: expected {counts}, got {first}')
    with duckdb.connect(str(database), read_only=True) as con:
        before = con.execute('select count(*) from landing.ingestion_ledger').fetchone()[0]
    second = run_ingestion(duckdb_path=str(database), hdfs_base_path=output,
                           webhdfs_url=webhdfs, staging_dir=str(work / 'staging'))
    with duckdb.connect(str(database), read_only=True) as con:
        after = con.execute('select count(*) from landing.ingestion_ledger').fetchone()[0]
        for topic, count in counts.items():
            if con.execute(f'select count(*) from landing.{topic}').fetchone()[0] != count:
                raise AssertionError(f'Ingestion rerun duplicated {topic}')
    if any(second.values()) or before != after:
        raise AssertionError('Ingestion rerun changed data/ledger')
    print('PASS ingestion rerun preserves counts and ledger', flush=True)
    project = work / 'dbt'
    shutil.copytree(ROOT / 'dbt/duckdb_warehouse', project,
                    ignore=shutil.ignore_patterns('target', 'logs', '__pycache__', '.user.yml', '*.duckdb*'))
    (work / 'profiles.yml').write_text(
        'duckdb_warehouse:\n  target: smoke\n  outputs:\n    smoke:\n      type: duckdb\n'
        f"      path: '{database.as_posix()}'\n      schema: main\n      threads: 1\n", encoding='utf-8')
    executable = Path(sys.executable).with_name('dbt.exe' if os.name == 'nt' else 'dbt')
    log = work / 'dbt-build.log'
    with log.open('w', encoding='utf-8') as stream:
        subprocess.run([str(executable), 'build', '--project-dir', str(project),
                        '--profiles-dir', str(work), '--target', 'smoke'],
                       stdout=stream, stderr=subprocess.STDOUT, timeout=240, check=True)
    with duckdb.connect(str(database), read_only=True) as con:
        result = con.execute('select count(*), count(distinct user_id), sum(listened_seconds) '
                             'from dashboard.song_plays').fetchone()
        if result != (4, 2, 360.0):
            raise AssertionError(f'Dashboard fixture expected (4,2,360), got {result}')
    # Exercise the same read-only query layer used by the dashboard UI.
    sys.path.insert(0, str(ROOT / 'streamlit'))
    from reports import load_metadata, load_report
    metadata = load_metadata(database)
    summary = load_report(database, date(2026, 10, 16), date(2026, 10, 16))['summary'].iloc[0]
    if metadata['total_rows'] != 4 or (int(summary['plays']), int(summary['listeners']),
                                      float(summary['known_seconds'])) != (4, 2, 360.0):
        raise AssertionError('Dashboard read-only query layer did not refresh the test data')
    print(f'PASS dbt build and dashboard: 4 plays, 2 users, 360 seconds; {database}', flush=True)


def start_isolated_hdfs(work, run_id):
    """Fresh HDFS for verification; never mount or format the existing storage.

    The existing WebHDFS client maps datanode:9864 to host port 9864, so
    that host port must be free. The dedicated Docker network owns the alias.
    """
    network = f'cluster-hdfs-smoke-{run_id}'
    namenode = f'namenode-smoke-{run_id}'
    datanode = f'datanode-smoke-{run_id}'
    conf = work / 'hadoop'
    conf.mkdir()
    def xml(properties):
        return '<configuration>' + ''.join(
            f'<property><name>{escape(k)}</name><value>{escape(v)}</value></property>'
            for k, v in properties.items()) + '</configuration>'
    (conf / 'core-site.xml').write_text(xml({'fs.defaultFS': f'hdfs://{namenode}:9000',
        'hadoop.http.staticuser.user': 'root'}), encoding='utf-8')
    (conf / 'hdfs-site.xml').write_text(xml({'dfs.replication': '1',
        'dfs.namenode.name.dir': 'file:///opt/hadoop/data/nameNode',
        'dfs.datanode.data.dir': 'file:///opt/hadoop/data/dataNode',
        'dfs.webhdfs.enabled': 'true', 'dfs.permissions.enabled': 'false',
        'dfs.datanode.use.datanode.hostname': 'true', 'dfs.client.use.datanode.hostname': 'true'}), encoding='utf-8')
    docker('network', 'create', network)
    common = ['--network', network, '--user', '0', '--mount',
              f'type=bind,source={conf},target=/opt/hadoop/etc/hadoop-smoke,readonly',
              '-e', 'HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop-smoke']
    try:
        docker('run', '-d', '--name', namenode, '--hostname', namenode, *common,
               '-p', '127.0.0.1:19870:9870', '--mount',
               f'type=volume,source={network}-name,target=/opt/hadoop/data/nameNode',
               'apache/hadoop:3.3.6', 'bash', '-c',
               'if [ ! -d /opt/hadoop/data/nameNode/current ]; then /opt/hadoop/bin/hdfs namenode -format; fi && exec /opt/hadoop/bin/hdfs namenode')
        docker('run', '-d', '--name', datanode, '--hostname', 'datanode', '--network-alias', 'datanode',
               *common, '-p', '127.0.0.1:9864:9864', '--mount',
               f'type=volume,source={network}-data,target=/opt/hadoop/data/dataNode',
               'apache/hadoop:3.3.6', '/opt/hadoop/bin/hdfs', 'datanode')
        def live():
            response = requests.get('http://127.0.0.1:19870/jmx',
                params={'qry': 'Hadoop:service=NameNode,name=FSNamesystemState'}, timeout=5)
            response.raise_for_status()
            return response.json()['beans'][0]['NumLiveDataNodes'] == 1
        wait_for(live, 'isolated HDFS DataNode registration', seconds=90)
    except Exception:
        for name in (datanode, namenode):
            try:
                docker('stop', '-t', '10', name)
            except Exception:
                pass
        raise
    return network, namenode, datanode


def cleanup_smoke(work, container, hdfs):
    # Log collection must never prevent a stop attempt for ANY owned container.
    try:
        (work / 'spark.log').write_text(docker('logs', '--tail', '250', container), encoding='utf-8')
    except Exception as exc:
        print(f'Cannot collect smoke log: {container}: {exc}', file=sys.stderr)
    for name in [container, *(hdfs[1:] if hdfs else ())]:
        try:
            docker('stop', '-t', '15', name)
        except Exception as exc:
            print(f'Check smoke container cleanup manually: {name}: {exc}', file=sys.stderr)


def run_smoke(recovery=True, build_downstream=True, isolated_hdfs=False):
    load_dotenv(ROOT / '.env')
    run_id = uuid.uuid4().hex[:12]
    work = ROOT / 'data' / 'cluster-smoke' / run_id
    work.mkdir(parents=True)
    output = f'/data/events-kafka-v2-test/{run_id}'
    checkpoint = f'/checkpoint/kafka-v2-test/{run_id}'
    prefix = f'cluster_pipeline_{run_id}_'
    container = f'spark-cluster-smoke-{run_id}'
    webhdfs = os.getenv('WEBHDFS_URL', 'http://127.0.0.1:9870/webhdfs/v1').rstrip('/')
    bootstrap = bootstrap_servers()
    hdfs = None
    hdfs_url = 'hdfs://namenode:9000'
    fixture = samples()
    ensure_kafka_topics(bootstrap, [prefix + t for t in fixture])
    expected = {t: produce(bootstrap, prefix + t, rows) for t, rows in fixture.items()}
    manifest = dict(container=container, output=output, checkpoint=checkpoint,
                    topic_prefix=prefix, work=str(work), expected_counts={t: len(v) for t, v in fixture.items()})
    (work / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(f'Smoke artifacts: {work}', flush=True)
    try:
        if isolated_hdfs:
            hdfs = start_isolated_hdfs(work, run_id)
            hdfs_url = f'hdfs://{hdfs[1]}:9000'
            webhdfs = 'http://127.0.0.1:19870/webhdfs/v1'
            manifest['isolated_hdfs'] = hdfs
            (work / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        docker('create', '--name', container, '--network', 'eventsim-net',
               '--mount', f'type=bind,source={ROOT / "spark"},target=/app/spark,readonly',
               '--mount', f'type=bind,source={ROOT / "jars"},target=/app/jars,readonly',
               '-e', 'KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092',
               '-e', 'PYTHONUNBUFFERED=1', '-e', f'HDFS_URL={hdfs_url}',
               '-e', f'HDFS_BASE_PATH={output}', '-e', f'HDFS_CHECKPOINT_PATH={checkpoint}',
               '-e', f'KAFKA_TOPIC_PREFIX={prefix}', 'spark-streaming:latest')
        if hdfs:
            docker('network', 'connect', hdfs[0], container)
        docker('start', container)
        wait_for(lambda: verify_offsets(webhdfs, output, expected), 'Spark initial committed records', seconds=150)
        print('PASS source -> both Kafka partitions -> committed HDFS Parquet: counts and offsets match', flush=True)
        if recovery:
            docker('stop', '-t', '15', container)
            # Same event payload at new Kafka offsets is deliberate: Spark appends
            # offsets, while staging may later deduplicate identical business events.
            for topic, rows in fixture.items():
                expected[topic].update(produce(bootstrap, prefix + topic, rows))
            docker('start', container)
            wait_for(lambda: verify_offsets(webhdfs, output, expected), 'Spark resumed offsets without replay', seconds=150)
            print('PASS Spark checkpoint recovery: all new offsets present exactly once', flush=True)
        if build_downstream:
            downstream(work, output, webhdfs, {t: len(v) for t, v in expected.items()})
        (work / 'result.json').write_text(json.dumps({'status': 'PASS', **manifest}, indent=2), encoding='utf-8')
    finally:
        cleanup_smoke(work, container, hdfs)
    return work


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--no-recovery', action='store_true')
    parser.add_argument('--no-downstream', action='store_true')
    parser.add_argument('--isolated-hdfs', action='store_true',
                        help='Fresh HDFS test volumes; requires free host ports 19870 and 9864')
    args = parser.parse_args()
    run_smoke(not args.no_recovery, not args.no_downstream, args.isolated_hdfs)


if __name__ == '__main__':
    main()
