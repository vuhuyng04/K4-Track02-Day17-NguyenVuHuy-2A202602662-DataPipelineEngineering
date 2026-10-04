# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Vũ Huy / 2A202602662
**Repo:** https://github.com/vuhuyng04/K4-Track02-Day17-NguyenVuHuy-2A202602662-DataPipelineEngineering
**Commit bài nộp:** HEAD của nhánh `main` lúc nộp (ba commit sửa lỗi: `ea6e95b`, `d17d9e7`, `28ddcc3`)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Anthropic) — đọc code, chỉ ra vị trí 3 lỗi, đề xuất và viết bản sửa, viết bước cache LLM (B1), soạn DESIGN.md (B2), chạy các lệnh kiểm tra. Tôi đã review từng thay đổi và tự chạy lại toàn bộ kiểm tra; output bên dưới sinh từ code trong repo.
**Nguồn tham khảo khác (nếu có):** slide Ngày 17; tài liệu Debezium (định dạng event Postgres), dbt docs (incremental merge, microbatch).

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | verify: `24 rows for 12 tickets`; T-91 có 3 hàng (`low/open`, `high/open`, `high/closed/bug`) | feature_daily ≠ full recompute (`c50b…` ≠ `8630…`); u05 ngày 08-12 = `(2, 0)` thay vì `(5, 1)` | T-97 vẫn `is_deleted=False`, còn `u06`/subject/body; còn trong snapshot mới nhất và 2 chunk RAG |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT`: mỗi batch append, không khoá, không so LSN | `LOOKBACK_DAYS = 0` (đoán "event đến trong vài giây"); event trễ 3 ngày không được tính lại vào ngày gốc | `ticket_id` lấy từ `after`; delete có `after = null` nên bị `WHERE ticket_id IS NOT NULL` loại |
| **Cách sửa** | `silver.py`: `MERGE … ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT` | `config.py`: `LOOKBACK_DAYS = 3` = ceil(P99) đo bằng `main.py --lateness` | `staging.py`: `coalesce(after.ticket_id, before.ticket_id)` → tombstone (PII null, giữ LSN); Gold đã lọc `_op <> 'd'` / `NOT is_deleted` |
| **Khái niệm** | Silver có khoá; MERGE + LSN guard | Data về muộn; event time vs ingest time; lookback = ceil(P99) | CDC log-based; tombstone; "xoá phải lan" |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50=0, p95=2.90, n=43) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY**

## 3. Lựa chọn công cụ / kỹ thuật

- MERGE cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: ticket là thực thể đổi trạng thái, mỗi batch chỉ chứa một phần nên phải upsert theo khoá + LSN để batch cũ không đè bản mới; feature là aggregate theo ngày nên tính lại trọn `[D-3, D]` từ Silver đơn giản và idempotent.
- Tombstone thay vì xoá hẳn: hàng giữ `ticket_id` + LSN của lần xoá nên replay CDC cũ không làm ticket hồi sinh, PII đã bị xoá; đánh đổi là một hàng rỗng tồn tại mãi.
- Snapshot dựng lại từ Bronze "as of" ngày đó: huấn luyện phải tái lập được, nên `v<day>` bất biến (kiểm checksum), thay đổi chỉ vào version mới.
- DuckDB/dbt thay vì Spark: vài chục nghìn hàng chạy trong một tiến trình; dbt thêm merge/microbatch, contract, test trên cùng DuckDB.

## 4. Hai câu hỏi suy ngẫm

1. Bất biến để tái lập, không phải để giữ PII mãi. Khi có yêu cầu xoá: tạo version thay thế cho các snapshot chứa T-97 (không có T-97), đánh dấu bản cũ `retired`, xoá vật lý sau hạn lưu trữ và ghi log lý do. Về lâu dài dùng crypto-shredding (mã hoá text theo khoá từng user, xoá = huỷ khoá) để khỏi viết lại file.
2. Đặt chốt ở ranh giới Bronze → Silver: thêm NER tiếng Việt (underthesea/PhoBERT-NER) sau regex, thay tên bằng `<NAME>`, hạn chế quyền đọc Bronze. Đo recall/precision trên ~200 ticket gán nhãn tay (mục tiêu recall tên ≥ 95%), thêm contract quét Silver/Gold cảnh báo khi PII lọt.
## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell (Python 3.13, `.venv`), lệnh tương đương theo SUBMISSION.md.

```text
$ .\.venv\Scripts\python.exe -m scripts.verify          # make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest                   # make test
..................................                                       [100%]
34 passed in 2.51s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check      # make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness          # make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ .\.venv\Scripts\python.exe main.py --land-only; cd dbt_project
$ ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17   # make dbt
03:19:57  Running with dbt=1.12.5
03:19:57  Registered adapter: duckdb=1.11.0
03:19:59  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
03:19:59  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.06s]
03:19:59  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
03:19:59  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.07s]
03:20:00  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.12s]
03:20:00  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.09s]
03:20:00  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.03s]
03:20:00  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
03:20:00  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
03:20:00  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.01s]
03:20:00  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.04s]
03:20:00  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
03:20:00  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
03:20:00  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
03:20:00  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
03:20:00  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
03:20:00  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.20s]   (7 of 7 batches OK)
03:20:00  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
03:20:00  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
03:20:00  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
03:20:00  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.07 seconds (1.07s).
03:20:00  Completed successfully
03:20:00  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity           # make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

(Output dbt đã lược các dòng `START … [RUN]`; mọi dòng kết quả giữ nguyên.)

### Bonus B1 — LLM step (`pipeline/llm_label.py`)

```text
$ .\.venv\Scripts\python.exe -m scripts.bonus_llm        # make bonus-llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

Cache key = `sha256(subject. body)` + model + prompt version (bảng `llm_label_cache`); chi phí ước lượng cho các ticket chưa cache trước khi gọi model; câu trả lời sai schema được cache (không trả tiền lại) nhưng chỉ vào `llm_label_quarantine`, không vào `gold_ticket_labels`.

### Bonus B2

- Brainstorm: [`bonus/DESIGN.md`](../bonus/DESIGN.md).
- Airflow 3: bằng chứng trong [`bonus/airflow/`](../bonus/airflow/) — ảnh 7 run backfill
  (08-10 → 08-16) đều Success, ảnh backfill, log `backfill create` và checksum.

```text
$ docker compose -f docker/docker-compose.yml exec -w /opt/airflow/dags airflow python -c "...gold_checksums..."
{'gold_feature_daily': '8630e04a61d10aa963b7a49e148926b0', 'gold_training_set': '9370ca77af233dfc2640e8cb61ef12d1', 'gold_doc_chunks': 'cb9ebd12fdccafa4b746141147471964', 'gold': '39e115c510ecdf526800eac227158a4f'}
```
Gold checksum của Airflow = checksum fresh build `39e115c510ecdf526800eac227158a4f`.
