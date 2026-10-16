"""Run the isolated sample-to-dashboard smoke, without restarting Spark.

Legacy checks based on any existing Parquet/container state were replaced by
exact fixture offset/count reconciliation. This does not claim quarantine support.
"""
import sys
from pipeline_smoke import run_smoke

if __name__ == '__main__':
    try:
        run_smoke(recovery=False, build_downstream=True)
    except Exception as exc:
        print(f'FAIL: {exc}', file=sys.stderr)
        sys.exit(1)
