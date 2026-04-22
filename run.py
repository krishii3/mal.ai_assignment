from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
VENV_DIR = ROOT_DIR / ".venv"
REQUIREMENTS = ROOT_DIR / "requirements.txt"


def _venv_python() -> Path:
    scripts_dir = "Scripts" if os.name == "nt" else "bin"
    python_name = "python.exe" if os.name == "nt" else "python"
    return VENV_DIR / scripts_dir / python_name


def _system_python() -> str:
    candidates = []
    if os.name == "nt":
        launcher = shutil.which("py")
        if launcher:
            return launcher
        candidates.extend(["python", "python3"])
    else:
        candidates.extend(["python3", "python"])

    for candidate in candidates:
        if shutil.which(candidate):
            return candidate

    raise SystemExit("Python 3.9+ is required but no system Python executable was found.")


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT_DIR, check=check, text=True)


def _ensure_venv() -> Path:
    venv_python = _venv_python()
    if venv_python.exists():
        return venv_python

    system_python = _system_python()
    create_cmd = [system_python]
    if os.name == "nt" and Path(system_python).name.lower() == "py":
        create_cmd.append("-3")
    create_cmd.extend(["-m", "venv", str(VENV_DIR)])
    _run(create_cmd)
    return venv_python


def _ensure_dependencies(venv_python: Path) -> None:
    probe = subprocess.run(
        [str(venv_python), "-m", "pip", "show", "pandas"],
        cwd=ROOT_DIR,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    if probe.returncode != 0:
        _run([str(venv_python), "-m", "pip", "install", "-r", str(REQUIREMENTS)])


def _run_pipeline(venv_python: Path, extra_args: list[str]) -> int:
    _run([str(venv_python), "-m", "src.main", *extra_args])
    _run([str(venv_python), str(ROOT_DIR / "docs" / "generate_pdf.py")])
    return 0


def _run_dashboard(venv_python: Path, extra_args: list[str]) -> int:
    command = [str(venv_python), "-m", "streamlit", "run", str(ROOT_DIR / "streamlit_app.py"), *extra_args]
    completed = subprocess.run(command, cwd=ROOT_DIR, check=False)
    return completed.returncode


def _print_sql_queries() -> int:
    sys.stdout.write((ROOT_DIR / "sql" / "analytical_queries.sql").read_text())
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the payment pipeline, dashboard, or analytical SQL output.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--dashboard", "-db", action="store_true", help="Launch the Streamlit dashboard.")
    group.add_argument("--sql-queries", "-sql", action="store_true", help="Print the analytics SQL queries.")
    parser.add_argument("extra_args", nargs=argparse.REMAINDER, help="Extra args forwarded to the selected mode.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    extra_args = list(args.extra_args)
    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]

    if args.sql_queries:
        return _print_sql_queries()

    venv_python = _ensure_venv()
    _ensure_dependencies(venv_python)

    if args.dashboard:
        return _run_dashboard(venv_python, extra_args)
    return _run_pipeline(venv_python, extra_args)


if __name__ == "__main__":
    raise SystemExit(main())
