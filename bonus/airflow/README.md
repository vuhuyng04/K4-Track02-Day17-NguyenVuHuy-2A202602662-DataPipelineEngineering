# Bonus B2 — Airflow 3 evidence

Airflow `apache/airflow:3.3.2` standalone via `docker compose -f docker/docker-compose.yml up -d`.

```text
airflow dags unpause day17_support_pipeline
airflow backfill create --dag-id day17_support_pipeline --from-date 2026-08-10 --to-date 2026-08-16 \
    --max-active-runs 1 --reprocess-behavior failed          # dry-run first, then for real
```

- `01_dag_runs_7_success.png` — 7 backfill runs (2026-08-10 .. 2026-08-16), all **Success**;
  grid shows `land_bronze → build_silver → build_gold` green for every run.
- `02_backfill.png` — the backfill object (Missing and Errored Runs, max active runs 1, completed).
- `backfill_create.log` — output of `airflow backfill create`.
- `checksum_airflow.txt` — Gold checksums of the container warehouse after the backfill.

| | gold (combined) |
|---|---|
| Airflow backfill (container warehouse) | `39e115c510ecdf526800eac227158a4f` |
| Fresh build on host (`submission/checksums.txt`) | `39e115c510ecdf526800eac227158a4f` |

Same code path, same Bronze → identical Gold checksum.
