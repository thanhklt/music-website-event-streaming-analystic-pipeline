# Execution ledger — plan: docs/superpowers/plans/2026-10-16-kafka-three-broker-cluster.md

User approved implementation in the current working tree on 2026-10-16.
No stage, commit, push, PR, deployment to remote, or deletion of existing data is authorized.

## Pre-flight

- Tasks 1 → 2 → 3 → 4: INTERNAL/EXTERNAL bootstrap endpoints and topic policy agree: 3 brokers, 2 partitions, RF=2, min ISR=2.
- Tasks 1 → 3 → 5: output and checkpoint namespaces must move together; ingestion must consume the new output path.
- Existing user edits: .env.example, .gitignore, AGENTS.md, README.md, pyproject.toml; untracked docs, poetry.lock, streamlit and a seed. Preserve all unrelated edits.
- Docker access works with sandbox escalation. Existing Kafka/HDFS/DuckDB volumes exist; no eventsim-kafka-v2-* volumes found at pre-flight.
- Ruling: Work in the current checkout, including main, as explicitly described in the approved plan; do not create/stage/commit worktrees. Cost if wrong: user reviews local diffs on the current branch.
- Ruling: Keep this progress document rather than commit-oriented skill helper bookkeeping; project instructions prohibit commits. Cost: the ledger remains a reviewable local document.

## Status

- Task 1: in progress.
- Task 2–5: pending.

- Ruling: Factor import-safe Kafka policy/bootstrap helpers into spark/kafka_cluster.py to share Spark, sample producer and live smoke validation without starting Spark during tests. Cost: one small additional module.
- Ruling: Advertise EXTERNAL as 127.0.0.1 instead of localhost; Windows selected ::1 while Docker binds IPv4 loopback (TCP IPv4 passed, IPv6 failed). Cost: external clients must use local IPv4 as documented; ports are unchanged.
- Task 1: Compose validation passed; three fresh volumes created; all three brokers healthy; quorum has voters 1,2,3, lag 0; cluster ID and log mount persisted in eventsim-kafka-v2-1-data. Existing Kafka container remains stopped and unchanged.
- Task 2: Eight offline regression cases passed after expected missing-module RED; live provisioning/policy validation passed via all three host endpoints. Fixed TopicCollection import against installed package source.

- Task 3: PowerShell parser and Bash bash -n passed. Eventsim image help and a 2-user/5-minute sample passed; one page_view event acknowledged. Observed assembly Kafka client2.6.0 uses acks=1; README states limitation explicitly.
- Task 4: Broker fault/restart and live wrong-policy rejection passed. Reviewer found false-PASS risk for generic transport failure and cleanup skipping stop after log errors. Both regressions were observed RED; fixed, offline suite now10/10. Fault test now forces kafka-1 loss, waits for surviving partition leader, accepts only NOT_ENOUGH_REPLICAS(/AFTER_APPEND), and checks INTERNAL and EXTERNAL bootstrap lists.
- Task 5: Isolated HDFS full smoke run fa02ab88f087 PASS: initial offsets, checkpoint restart with every new offset exactly once, landing auth/listen/page/status4/8/4/4 after deliberate replay at new offsets, ingestion ledger16 files unchanged on rerun, dbt218 PASS/0 WARN/0 ERROR, dashboard query layer4 plays/2 listeners/360s.
- Incident: Starting existing NameNode at15:58UTC triggered its pre-existing wrong-path guard and -format -force against /opt/hadoop/dfs/name. DataNode then failed incompatible clusterID. The assistant disclosed this effect and apologized; old metadata was not confirmed recoverable. An attempted stop was rejected by automatic approval review; no bypass was performed.
- User update: explicitly does not need old metadata, will rerun pipeline, then explicitly requested deletion of old volumes. This supersedes original preserve-old-volumes constraints for retired Kafka/HDFS only.
- Ruling: Create fresh eventsim-hdfs-v2-name/data and align mounts and format guard to configured paths. Add root hdfs-init that chowns ONLY mounted directory roots so normal daemon stays hadoop; refuse formatting nonempty storage lacking VERSION. Cost: deliberate fresh HDFS, consistent with updated user request. No old database full-refresh.
- Ruling: Replace legacy pipeline/recovery entrypoints with isolated deterministic fixture runners rather than preserving prior container-alive/any-Parquet false positives and missing input-file dependencies. Cost: old helper functions are no longer an API; entrypoint commands remain.
- Cleanup preflight found two old anonymous /data volumes reused by Compose container recreation; initial deletion aborted BEFORE any removals. Renew anonymous volumes only after standard HDFS smoke completes; then delete the exact verified retirement list, not a prune.
