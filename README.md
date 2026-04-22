# Mal — Unified Payment Data Pipeline

A unified canonical payment event schema plus a Python ingestion pipeline that consolidates three product squads' payment data (**Cards**, **Transfers**, **Bill Payments**) into a single contract. Built for an Islamic banking fintech context (UAE, Mal).

> This is Part 1 of the assignment. The architecture & migration document (Part 2) lives at [docs/architecture.md](docs/architecture.md) and is rendered to [docs/architecture.pdf](docs/architecture.pdf) via `python docs/generate_pdf.py`.

---

## What this does

1. **Ingests** three realistic mock CSVs under [data/raw/](data/raw/), each written in the style of its originating squad:
   - `cards_squad.csv` — ISO 8583-inspired card transactions
   - `transfers_squad.csv` — UAEFTS/SWIFT-style transfers
   - `bills_squad.csv` — Bawabat-style bill payments
2. **Validates** with two layers: Pandera (DataFrame) → Pydantic v2 (per-record). Rejected records land in `data/output/validation_errors.json` — nothing is silently dropped.
3. **Transforms** each squad's records into a single canonical `PaymentEventV1` schema (see [src/schema/v1.py](src/schema/v1.py)).
4. **Migrates** v1 → v2 (see [src/schema/v2.py](src/schema/v2.py) and [src/migration/v1_to_v2.py](src/migration/v1_to_v2.py)) demonstrating both non-breaking additions (`risk_score`, `product_type`, `profit_rate`) and a breaking rename (`extended_attributes` → `payment_metadata`).
5. **Writes** Parquet (columnar, compressed, schema-embedded) to `data/output/`.
6. **Exposes** DuckDB SQL queries in [sql/analytical_queries.sql](sql/analytical_queries.sql) so downstream teams can query Parquet directly.
7. **Visualizes** via an optional Streamlit dashboard ([streamlit_app.py](streamlit_app.py)).

---

## Setup

Python 3.9+.

The fastest path is the one-shot runner:

```bash
./run.sh
```

On Windows Command Prompt:

```bat
run.cmd
```

If you want to manage the environment manually:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows manual activation:

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

---

## Run the pipeline

Recommended:

```bash
./run.sh
```

Windows equivalent:

```bat
run.cmd
```

Manual equivalent:

```bash
python -m src.main
python docs/generate_pdf.py
```

Outputs land under `data/output/`:

```text
unified_payments_v1.parquet    canonical v1 events
unified_payments_v2.parquet    migrated v2 events
validation_errors.json         rejected records + reasons
pipeline_summary.json          run metadata
```

The sample data includes 2 intentionally broken rows: one negative amount in `cards_squad.csv` and one missing `customer_account_ref` in `bills_squad.csv`. These surface in the error report rather than being silently dropped.

---

## Query the unified model (DuckDB)

```bash
duckdb -c "SELECT payment_type, COUNT(*) FROM 'data/output/unified_payments_v2.parquet' GROUP BY 1;"
```

Six worked examples (daily volume, failure rate, top billers, customer frequency, schema version rollout, currency mix) are in [sql/analytical_queries.sql](sql/analytical_queries.sql).

To print that SQL file directly:

```bash
./run.sh --sql-queries
```

Windows:

```bat
run.cmd --sql-queries
```

---

## Launch the dashboard (optional)

```bash
./run.sh --dashboard
```

Windows:

```bat
run.cmd --dashboard
```

Manual equivalent:

```bash
streamlit run streamlit_app.py
```

KPIs + Altair charts + an ad-hoc DuckDB SQL cell.

## Deploy the dashboard

GitHub Pages cannot host this app because Streamlit needs a running Python process. Use Streamlit Community Cloud for the dashboard, and use GitHub Pages only for static project pages if you want a landing page or documentation.

Recommended deployment steps:

1. Push the repository to GitHub.
2. In Streamlit Community Cloud, create a new app from the repo.
3. Set the main file path to `streamlit_app.py`.
4. Keep `requirements.txt` in the repo root so Streamlit can install dependencies automatically.

The app now generates its own demo Parquet outputs on first start, so it can boot in a fresh deployment without committed files under `data/output/`.

This repo also includes a GitHub Actions workflow at `.github/workflows/streamlit-deploy.yml` that installs dependencies, builds the pipeline outputs, starts Streamlit in headless mode, and performs a simple health check on every push to `main` and on pull requests.

---

## Generate the architecture PDF

```bash
python docs/generate_pdf.py
# writes docs/architecture.pdf
```

---

## Project layout

```text
demta/
├── data/
│   ├── raw/                        mock input CSVs (3 squads)
│   └── output/                     pipeline output (Parquet + JSON)
├── src/
│   ├── schema/                     canonical Pydantic models (v1, v2)
│   ├── pipeline/                   ingest + transform + validate
│   ├── migration/                  v1 → v2 migration
│   └── main.py                     orchestrator
├── sql/analytical_queries.sql      DuckDB queries for downstream teams
├── streamlit_app.py                optional dashboard
├── docs/
│   ├── architecture.md             Part 2 document (source)
│   └── generate_pdf.py             fpdf2 renderer
├── requirements.txt
└── README.md
```

---

## Design decisions (short form — long form in docs/architecture.md)

| Choice | Why | What we rejected |
| --- | --- | --- |
| Plain Python orchestration | Runnable locally with one command; under line budget | Airflow/Prefect (overkill for MVP) |
| Pydantic v2 + Pandera | Record-level + DataFrame-level checks | Great Expectations (monitoring tool, 500+ line setup) |
| Parquet + DuckDB | Columnar, schema-embedded, zero-install SQL | Postgres/SQLite (row-oriented; server setup) |
| Flat core + JSON metadata | Clean queries, low NULL sprawl, extensible | Inheritance tables (join-heavy) |
| Semver + versioned Pydantic classes | Explicit migration code is readable | Avro + Schema Registry (streaming-era tooling) |
| Islamic banking fields first-class | `profit_rate` (not `interest_rate`), `product_type`, `is_shariah_compliant` — aligned with AAOIFI | Generic schema with retrofitted Shariah fields later |

---

## Assignment constraints

- Python 3.9+ ✓
- `requirements.txt` present ✓
- Mock data covers amount, currency, timestamp, status, customer_id, payment_method ✓
- Runnable locally with one command ✓
- Total Python (pipeline) < 500 lines ✓ (see `wc -l src/**/*.py src/main.py`)

Streamlit dashboard and PDF generator are presentation artifacts; they sit outside the 500-line pipeline budget.

### Single command run

```bash
./run.sh
```

```bat
run.cmd
```

### Runner options

```bash
./run.sh --dashboard
./run.sh --sql-queries
./run.sh --help
```

```bat
run.cmd --dashboard
run.cmd --sql-queries
run.cmd --help
```

Extra args can be forwarded after `--`:

```bash
./run.sh -- --example-arg
```

```bat
run.cmd -- --example-arg
```
