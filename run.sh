#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PYTHON="$ROOT_DIR/.venv/bin/python"
MODE="pipeline"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dashboard|-db)
            MODE="dashboard"
            shift
            ;;
        --sql-queries|-sql)
            MODE="sql-queries"
            shift
            ;;
        --help|-h)
            cat <<'EOF'
Usage: ./run.sh [--dashboard | --sql-queries] [extra args]

Default behavior:
    Run the payment pipeline and regenerate the architecture PDF.

Flags:
    --dashboard, -db       Launch the Streamlit dashboard.
    --sql-queries, -sql   Print the analytics SQL queries.
    --help, -h            Show this help message.

Examples:
    ./run.sh
    ./run.sh --dashboard
    ./run.sh --sql-queries
    ./run.sh -- --example-arg
EOF
            exit 0
            ;;
        --)
            shift
            EXTRA_ARGS+=("$@")
            break
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

cd "$ROOT_DIR"

if [[ ! -x "$VENV_PYTHON" ]]; then
    python3 -m venv "$ROOT_DIR/.venv"
fi

if ! "$VENV_PYTHON" -m pip show pandas >/dev/null 2>&1; then
    "$VENV_PYTHON" -m pip install -r "$ROOT_DIR/requirements.txt"
fi

run_pipeline() {
    "$VENV_PYTHON" -m src.main "$@"
    "$VENV_PYTHON" "$ROOT_DIR/docs/generate_pdf.py"
}

exec_with_optional_args() {
    local cmd=("$@")
    if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
        cmd+=("${EXTRA_ARGS[@]}")
    fi
    exec "${cmd[@]}"
}

case "$MODE" in
    pipeline)
        if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
            run_pipeline "${EXTRA_ARGS[@]}"
        else
            run_pipeline
        fi
        ;;
    dashboard)
        if ! "$VENV_PYTHON" -m pip show streamlit >/dev/null 2>&1; then
            "$VENV_PYTHON" -m pip install -r "$ROOT_DIR/requirements.txt"
        fi
        exec_with_optional_args "$VENV_PYTHON" -m streamlit run "$ROOT_DIR/streamlit_app.py"
        ;;
    sql-queries)
        exec cat "$ROOT_DIR/sql/analytical_queries.sql"
        ;;
esac
