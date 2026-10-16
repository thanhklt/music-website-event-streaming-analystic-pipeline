"""Verify Spark checkpoint recovery using isolated Kafka/HDFS artifacts.

The test matches every acknowledged Kafka (partition, offset) to exactly one
committed Parquet row before and after restart. It never stops the user's
spark-streaming container or uses an existing warehouse/checkpoint.
"""
import sys
from pipeline_smoke import run_smoke

if __name__ == '__main__':
    try:
        run_smoke(recovery=True, build_downstream=False)
    except Exception as exc:
        print(f'FAIL: {exc}', file=sys.stderr)
        sys.exit(1)
