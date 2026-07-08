# tools/ — auxiliary & ad-hoc scripts

Not part of the scheduled pipeline (no cron entry / systemd service). One-off
diagnostics, manual collectors, analysis helpers, and experiments. Run from the
repo root if they import project modules.

- audit_mismatch.py — NBCU-vs-feed mismatch diagnostic (one-off)
- chao_swarm_estimate.py — Chao1 true-swarm-size estimation (analysis)
- dht_ipc_writer.py — experimental single-writer IPC prototype
- dmca_site_discovery.py — streaming-site discovery (manual)
- domain_registry.py — streaming domain registry helper (manual)
- peer_query.py — ad-hoc canonical peer-count query tool
- stream_registry.py — streaming source registry (manual)
- streaming_liveness.py — streaming liveness probe (manual)
- transparency_ingest.py — transparency-report ingest (manual)
- velocity_rank.py — new-release velocity ranking (analysis)
