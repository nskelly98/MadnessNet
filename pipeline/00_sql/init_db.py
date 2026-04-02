# -*- coding: utf-8 -*-
"""
Created on Mon Jan 26 14:26:30 2026

@author: skell
"""
import sys
from pathlib import Path

# Add project root (pipeline/) to Python path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))


import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(r"C:\GoopNet")
DB_PATH = PROJECT_ROOT / "database.db"
SQL_DIR = PROJECT_ROOT / "pipeline" / "00_sql"

SQL_FILES = [
    SQL_DIR / "create_tables_core.sql",
    SQL_DIR / "create_tables_oddsapi.sql",
    SQL_DIR / "create_tables_tracking.sql",
]

def run_sql_file(conn: sqlite3.Connection, path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing SQL file: {path}")
    conn.executescript(path.read_text(encoding="utf-8"))

def main():
    PROJECT_ROOT.mkdir(parents=True, exist_ok=True)
    SQL_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    try:
        for f in SQL_FILES:
            print(f"Running SQL: {f}")
            run_sql_file(conn, f)
        conn.commit()
        print(f"OK: Initialized DB at {DB_PATH}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
