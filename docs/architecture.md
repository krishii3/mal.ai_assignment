# Mal — Unified Payment Data Pipeline

## Architecture & Migration Strategy

_Part 2 companion document. Target audience: Mal Data Platform lead + product-squad tech leads (Cards, Transfers, Bill Payments). UAE context, Islamic banking._

---

## 1. Canonical Entity Design Rationale

**Schema shape.** The canonical `PaymentEvent` is a _flat core + typed metadata_ record. Core fields every squad produces — `payment_event_id`, `payment_type`, `amount`, `currency`, `status`, `customer_id`, `payment_method`, `timestamp` — sit at the top level. Squad-specific detail (merchant, beneficiary IBAN, biller code) lives inside `extended_attributes` (v1) / `payment_metadata` (v2). This is the pattern Stripe and Square use and it was chosen over two alternatives:

- **Fully flat schema with all squad columns:** every row would carry ~30 NULLs. Analysts lose confidence in the data, storage bloats, and adding a fourth payment type forces a schema migration.
- **Normalized inheritance (a `payments` table + `card_payments` / `transfer_payments` children):** clean on paper, but every downstream query needs three-way joins. In a UAE regulatory context (Central Bank reporting, AML) analysts need one-hop queries.

**Extensibility.** A new payment type (e.g. buy-now-pay-later, SmartPay, Aani real-time) means a new enum value in `payment_type` plus a new `transform_*` function. The contract doesn't move. Shariah-relevant fields (`is_shariah_compliant`, `product_type` in v2 with `MURABAHA` / `TAWARRUQ` / `IJARA` / `QARD` / `STANDARD`, `profit_rate` — not `interest_rate`, per AAOIFI) are first-class because Mal is an Islamic bank; bolting them on later would require a v3 rename.

**Trade-offs made.**

- `amount` is `Decimal(18,2)` — loses precision for sub-fils crypto settlement but keeps SQL `SUM()` safe and matches UAE Central Bank reporting precision.
- `extended_attributes` is a JSON dict, not a strictly-typed union. Cost: harder to statically validate. Benefit: 10x faster onboarding for a new squad — they fill a dict, no schema PR required.
- We settled on exactly 4 `status` values (`PENDING`, `COMPLETED`, `FAILED`, `REVERSED`). The Transfers squad originally had 7 (including `HOLD`, `AML_REVIEW`). Those collapse into `PENDING` with the original value preserved in metadata — simpler for analytics, loss-less for compliance.

---

## 2. Phased Migration Plan (30 / 60 / 90 days)

| Window | Goal | Milestones | Adoption sequence |
|---|---|---|---|
| **Days 0–30 — Foundation** | Contract frozen, Bill Payments squad onboarded first. | Schema v1 published; CI-enforced contract tests; Bill Payments writes canonical events alongside their legacy pipeline (dual-write, idempotent). | Bills first — smallest squad, most homogeneous data (Bawabat/UAE Direct Debit), lowest risk. Win here → credibility for the next two. |
| **Days 31–60 — Expansion** | Cards + Transfers onboarded; analytics consumers pointed at canonical Parquet. | Cards squad completes dual-write; Transfers squad completes dual-write; Finance & Risk dashboards migrated to read from `unified_payments_v2.parquet`; legacy pipelines enter read-only "deprecated" mode. | Cards second — highest volume but most standardized internally (ISO 8583). Transfers last — highest schema variance (local + SWIFT + UAEFTS), benefits from lessons learned. |
| **Days 61–90 — Consolidation** | Legacy pipelines decommissioned; v2 contract live. | v1 → v2 migration function runs in production; squads delete their legacy transform jobs; contract governance council stood up; SLOs published. | All three squads on v2. |

**Backward compatibility during transition.** Dual-write windows are non-negotiable — no squad ever turns off the legacy pipeline before the canonical one has passed a 7-day reconciliation check (row count, AED sum, failure rate within 0.1%). v1 → v2 migration is implemented as a pure function (`src/migration/v1_to_v2.py`) so a v1-only producer can keep writing while downstream reads v2; the bridge runs inside the pipeline.

**Dependency management across squads.** One central Python package (`mal-payments-schema`) distributed via internal PyPI. Squads pin a minor version; breaking changes only ship on major bumps with a minimum 30-day coexistence window. Contract tests live in that package and every squad's CI runs them.

---

## 3. Data Contract & Governance

**Versioning.** SemVer for the schema. **Non-breaking** = adding an optional field with a default (bump minor, no action for consumers) — example in v2: `risk_score`, `product_type`, `profit_rate`. **Breaking** = renaming, removing, or tightening a type (bump major, required coexistence window) — example in v2: `extended_attributes` → `payment_metadata`. The migration function in `src/migration/v1_to_v2.py` shows both classes handled in one pass.

**Enforcement points.**

1. **Producer CI** — Pydantic `model_validate` on a sample of recent records before merge.
2. **Pipeline ingest** — Pandera bulk-column checks (cheap) then Pydantic per-record (strict). Rejected rows land in `validation_errors.json`, not `/dev/null`.
3. **Consumer side** — downstream BI reads a _view_ (`payments_v2_public`) that enforces not-null on the core columns.

**Ownership.** Data Platform owns the canonical schema definition and the migration functions. Each squad owns its producer transform (`transform_cards`, `transform_transfers`, `transform_bills`). Schema changes go through a lightweight RFC (one-pager template) approved by: (a) Data Platform lead, (b) all three squad tech leads, (c) the Shariah Compliance office whenever a field touches profit/product type. 72-hour SLA on reviews.

---

## 4. Adoption Metrics & Stakeholder Plan

**KPIs (measurable within 90 days).**

1. **Squad adoption rate** — % of squads dual-writing canonical events. Target: 100% by day 60.
2. **Canonical coverage** — share of total payment events written via the canonical pipeline. Target: 95% by day 90.
3. **Downstream migration** — # of dashboards / models reading from the canonical table vs. legacy. Target: 80% of top-20 dashboards by day 90.
4. **Data-quality error rate** — Pydantic validation failures / total events. Target: < 0.5% after stabilization.
5. **Schema change cycle time** — RFC submission → merge. Target: median < 5 business days.

**Communication plan.**

- **Weekly 30-min sync** during days 0–90, rotating agenda: one squad demos its producer, one downstream consumer demos its dashboard.
- **#payments-platform Slack channel** with a pinned changelog; every schema-touching PR auto-posts there.
- **Monthly showcase** to product & compliance leadership — lead with a specific win ("3 days to add a new BNPL payment type vs. historical 6 weeks").

**Handling resistance.** The Transfers squad already has a mature pipeline and will push back hardest. Concrete plays:

- **Frame the cost, not the change.** Show them: today, a new downstream consumer = 3 weeks of Transfers-team effort per squad × 3 squads. With canonical: one integration.
- **Don't force a rewrite.** Dual-write means their existing pipeline keeps running; we're only asking them to _additionally_ emit canonical events via a thin adapter.
- **Give them ownership.** Whichever squad lead chairs the governance council for the first quarter — probably Transfers, since they have the strongest opinions — gets a credible say in v2 design.
- **Set a regulatory deadline.** UAE Central Bank / Shariah board reporting already needs unified data; aligning the rollout with a compliance milestone removes the "why now" debate.

---

## 5. Production Considerations

**For 100K transactions/day** the current single-process pandas pipeline is fine (roughly 50–100s for 100K records); for 10× that we would change four things:

1. **Switch orchestration to Airflow or Prefect** with per-squad DAGs; today's `main.py` is a teaching artifact.
2. **Replace pandas with Polars or DuckDB-native** transforms inside the pipeline — 5–10× throughput, lower memory.
3. **Write to partitioned Parquet in object storage** (S3 / Azure Blob) partitioned by `CAST(timestamp AS DATE)` and `payment_type`, so downstream DuckDB/Athena scans stay cheap.
4. **Contract tests in a dedicated repo** with producer-team PRs auto-triggering them — Buf-style contract CI.

**Monitoring & alerting.**

- Pipeline-level: run duration, input-row counts per squad, validation error rate. Alert via PagerDuty when error rate > 1% or input rows deviate > 20% day-over-day.
- Business-level: currency mix, failure-rate per squad, AED volume vs. 7-day baseline (anomaly detection with a simple STL decomposition — no ML needed early).
- Data-freshness SLO: canonical table is ≤ 60 min behind source; burn a monthly error budget if it slips.

**What I intentionally cut from Part 1 and why.**

- **Real DB (Postgres / Snowflake):** Parquet + DuckDB demonstrates every analytical query the assignment asked for, without a server. DuckDB is the modern choice for local OLAP — SQLite is row-oriented and wrong for this workload.
- **Avro + Schema Registry:** streaming-era tooling. The assignment is a batch CSV pipeline; Pydantic + semver is the right-sized contract.
- **Airflow/Prefect:** adds Docker + webserver + scheduler for no testable gain inside a <500-line budget. Noted as the day-90+ production swap.
- **Great Expectations:** 500+ line setup for what Pandera + Pydantic already cover. GE is a monitoring suite, not a schema contract.
- **Auth / secrets:** mock CSVs, no real PII. In production every squad would sign events with an HMAC and the pipeline would verify.
- **Fuzz / property tests:** the Pydantic models are the contract; Hypothesis-based generators are a fast-follow, not an MVP requirement.
