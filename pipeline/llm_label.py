"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` is the NAIVE version: it calls the model for every
ticket on every run and writes whatever comes back. Your bonus task is to make
`python -m scripts.bonus_llm` print BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import hashlib
import json
import re

import duckdb

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Pull {"label": ...} out of the model's answer; None if it is not valid."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return None
    try:
        label = json.loads(m.group(0)).get("label")
    except json.JSONDecodeError:
        return None
    return label if label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def input_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """Cached, validated LLM labelling.

    Cache key = (hash(input text), model, prompt version): a re-run makes 0 calls,
    a new prompt version re-labels every ticket on purpose. Invalid answers are
    cached too (so a re-run does not pay for them again) but only reach
    llm_label_quarantine, never Gold.
    """
    model, prompt_version = llm.model, PROMPT_VERSION   # read at call time: versions can change
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR,
        label VARCHAR, raw VARCHAR)""")
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_quarantine (
        ticket_id VARCHAR, input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR,
        raw VARCHAR, reason VARCHAR)""")

    tickets = [(tid, text, input_hash(text)) for tid, text in live_tickets(con)]
    cached = {h for (h,) in con.execute(
        "SELECT input_hash FROM llm_label_cache WHERE model = ? AND prompt_version = ?",
        [model, prompt_version]).fetchall()}
    todo = [t for t in tickets if t[2] not in cached]

    # Estimate the cost BEFORE calling the model.
    est_tokens = estimate_tokens([text for _, text, _ in todo])
    est_usd = est_tokens / 1000 * PRICE_PER_1K_TOKENS_USD

    calls_before = llm.calls
    new_cache, new_quarantine = [], []
    for ticket_id, text, h in todo:
        raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
        label = parse_label(raw)
        new_cache.append((h, model, prompt_version, label, raw))
        if label is None:
            new_quarantine.append((ticket_id, h, model, prompt_version, raw,
                                   f"not valid JSON with label in {ALLOWED_LABELS}"))
    if new_cache:
        con.executemany("INSERT INTO llm_label_cache VALUES (?, ?, ?, ?, ?)", new_cache)
    if new_quarantine:
        con.executemany("INSERT INTO llm_label_quarantine VALUES (?, ?, ?, ?, ?, ?)",
                        new_quarantine)

    # Gold = live tickets x the cache for the CURRENT model + prompt, valid labels only.
    con.execute("""CREATE OR REPLACE TEMP TABLE _live_tickets (
        ticket_id VARCHAR, input_hash VARCHAR)""")
    if tickets:
        con.executemany("INSERT INTO _live_tickets VALUES (?, ?)",
                        [(tid, h) for tid, _, h in tickets])
    con.execute("""
        CREATE OR REPLACE TABLE gold_ticket_labels AS
        SELECT t.ticket_id, c.label, c.model, c.prompt_version
        FROM _live_tickets t
        JOIN llm_label_cache c
          ON c.input_hash = t.input_hash AND c.model = ? AND c.prompt_version = ?
        WHERE c.label IS NOT NULL
        ORDER BY t.ticket_id
    """, [model, prompt_version])
    (labeled,) = con.execute("SELECT count(*) FROM gold_ticket_labels").fetchone()
    return {"live": len(tickets), "cache_hits": len(tickets) - len(todo),
            "calls": llm.calls - calls_before, "est_tokens": est_tokens,
            "est_usd": round(est_usd, 6), "labeled": labeled,
            "quarantined": len(new_quarantine)}
