"""Sequential 3-batch TPC-DI orchestrator — the spec's real execution model.

Where ``run_benchmark.py`` does a single-pass load (one ``dbt run`` folds Batch1 +
Batch2/3 into the end-state warehouse), THIS driver runs the three batches the way the
TPC-DI spec intends: a historical load (Batch1), then two incremental CDC batches, with
a per-batch validation checkpoint written to ``DImessages`` between each. The finished
warehouse can then be validated against the COMPLETE Appendix-A audit
(``tools/run_sequential_audit.py``), including the DImessages-driven checks that the
single-pass end-state audit has to skip.

Execution model (per batch wall-clock is the benchmark output)::

    init:    create the Audit answer-key table + an empty DImessages log
    batch 1: dbt run --select tag:sequential --full-refresh --vars '{batch: 1}'
             batch_complete.sql  (Phase Complete Record -> DImessages)
             batch_validation.sql (per-batch validation rows -> DImessages)
    batch 2: dbt run --select tag:sequential,tag:incremental --vars '{batch: 2}'
             batch_complete + batch_validation
    batch 3: same, --vars '{batch: 3}'
    audit:   tools/run_sequential_audit.py  (exit nonzero on any FAIL)

The sequential and single-pass modes write the SAME warehouse table names, so they are
mutually exclusive per run; keep them in separate schemas (e.g. ``tpcdi3`` vs
``tpcdi3_seq``) via ``DBT_SCHEMA``. A sequential run resets its own state: batch 1 runs
``--full-refresh`` (rebuilding every table from Batch1) and the Audit/DImessages tables
are ``CREATE OR REPLACE``-d at init — no folder deletion, so it is OneLake-safe.

Reuses the same duckrun connection/env handling as scripts/run_queries.py
(``WAREHOUSE_PATH``, ``ONELAKE_TOKEN``, ``DBT_SCHEMA``).

Local::

    WAREHOUSE_PATH=./warehouse TPCDI_DIR=./staging DBT_SCHEMA=tpcdi3_seq \\
        python scripts/run_sequential.py --sf 3

OneLake::

    WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \\
      TPCDI_DIR=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Files/tpcdi/sf3 \\
      ONELAKE_TOKEN=... DBT_SCHEMA=tpcdi3_seq \\
      python scripts/run_sequential.py --sf 3 --target onelake
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

import duckrun

HERE = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(HERE)
SQLDIR = os.path.join(PROJ, "sql", "sequential")
# The sequential dbt project is fully separate from the single-pass project at the repo
# root — its own dbt_project.yml/profiles.yml/macros/models. Only the raw seed is shared.
SEQ_PROJ = os.path.join(PROJ, "sequential")

BATCHES = (1, 2, 3)

# Empty DImessages log, typed per the spec schema (see the upstream dw_init.sql
# CREATE TABLE DIMessages). Created empty; batch_complete/batch_validation append to it.
DIMESSAGES_DDL = """
CREATE OR REPLACE TABLE DImessages AS
SELECT
  CAST(NULL AS TIMESTAMP) AS MessageDateAndTime,
  CAST(NULL AS INTEGER)   AS BatchID,
  CAST(NULL AS VARCHAR)   AS MessageSource,
  CAST(NULL AS VARCHAR)   AS MessageText,
  CAST(NULL AS VARCHAR)   AS MessageType,
  CAST(NULL AS VARCHAR)   AS MessageData
WHERE 1 = 0
"""


def audit_ddl(seed: str) -> str:
    """CREATE OR REPLACE the Audit answer-key table from every Batch*/*_audit.csv.

    Mirrors tools/run_audit.py's ``load_audit`` (same positional PDGF layout and the
    ``try_cast(BatchID) IS NOT NULL`` header/junk filter) but materializes a persistent
    Delta table — the sequential batch_validation and the full audit both read it.
    """
    glob = f"{str(seed).rstrip('/')}/Batch*/*_audit.csv"
    read = (f"read_csv('{glob}', header=false, all_varchar=true, delim=',', "
            "quote='', escape='', nullstr='', null_padding=true, filename=false)")
    return f"""
CREATE OR REPLACE TABLE Audit AS
SELECT
  column0                    AS dataset,
  try_cast(column1 AS INTEGER) AS batchid,
  try_cast(column2 AS DATE)    AS date,
  column3                    AS attribute,
  try_cast(column4 AS BIGINT)  AS value,
  try_cast(column5 AS DOUBLE)  AS dvalue
FROM {read}
WHERE try_cast(column1 AS INTEGER) IS NOT NULL
"""


def connect(warehouse: str, schema: str):
    """A WRITABLE duckrun connection (Delta DML via ``conn.sql``), env handling as
    scripts/run_queries.py."""
    storage_options = None
    if warehouse.startswith("abfss://"):
        token = os.environ.get("ONELAKE_TOKEN", "")
        if not token:
            sys.exit("ERROR: ONELAKE_TOKEN is empty — needed to write an abfss:// warehouse")
        storage_options = {"bearer_token": token}
    # duckrun opens read-only by default; the orchestrator writes Audit/DImessages via DML.
    return duckrun.connect(
        warehouse, storage_options=storage_options, schema=schema, read_only=False)


def _split_statements(sql: str):
    """Split a ported .sql file into individual statements on ';'.

    Line comments (``--`` to end of line) are stripped first — otherwise a ';' inside a
    comment would be mistaken for a statement separator. These files are ours and carry
    no ``--`` inside string literals, so this is safe. Blank chunks are dropped.
    """
    lines = []
    for ln in sql.splitlines():
        i = ln.find("--")
        lines.append(ln if i < 0 else ln[:i])
    body = "\n".join(lines)
    return [s.strip() for s in body.split(";") if s.strip()]


def run_sql_file(conn, name: str, subs: dict) -> int:
    """Execute a ported operational SQL file (batch_complete / batch_validation) against
    the current schema, substituting ``{{token}}`` placeholders. Returns statements run."""
    path = os.path.join(SQLDIR, name)
    if not os.path.exists(path):
        print(f"     (skip: {name} not present yet)", flush=True)
        return 0
    with open(path, "r", encoding="utf-8") as fh:
        sql = fh.read()
    for k, v in subs.items():
        sql = sql.replace("{{" + k + "}}", str(v))
    n = 0
    for stmt in _split_statements(sql):
        conn.sql(stmt)
        n += 1
    return n


def dbt_run(batch: int, env: dict) -> None:
    # Batch 1 = --full-refresh over the whole sequential project (references + the
    # historical branch of every incremental model). Batches 2-3 select only the
    # bronze + dim/fact models (tag:incremental); the reference models are batch-1 static.
    cmd = ["dbt", "run", "--project-dir", SEQ_PROJ, "--profiles-dir", SEQ_PROJ,
           "--vars", f"{{batch: {batch}}}"]
    if batch == 1:
        cmd.append("--full-refresh")
    else:
        cmd += ["--select", "tag:incremental"]
    subprocess.run(cmd, check=True, env=env)


def dbt_test(env: dict) -> None:
    subprocess.run(
        ["dbt", "test", "--project-dir", SEQ_PROJ, "--profiles-dir", SEQ_PROJ],
        check=True, env=env)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, default=int(os.environ.get("TPCDI_SF", "3")))
    ap.add_argument("--target", choices=["local", "onelake"], default="local")
    ap.add_argument("--staging", default=os.environ.get("TPCDI_DIR",
                    os.path.join(PROJ, "staging")))
    ap.add_argument("--schema", default=os.environ.get("DBT_SCHEMA", "tpcdi_seq"))
    ap.add_argument("--skip-generate", action="store_true")
    ap.add_argument("--skip-test", action="store_true")
    ap.add_argument("--skip-audit", action="store_true")
    ap.add_argument("--batches", type=int, default=3, choices=(1, 2, 3),
                    help="run batches 1..N (default 3; use 1 to validate the historical "
                         "load before the incremental branches are complete)")
    ap.add_argument("--init-only", action="store_true",
                    help="create Audit + DImessages and exit (skeleton smoke test)")
    ap.add_argument("--force", action="store_true", help="force data regeneration")
    args = ap.parse_args()

    staging = args.staging if str(args.staging).startswith("abfss://") \
        else os.path.abspath(args.staging)
    env = dict(os.environ)
    env["TPCDI_DIR"] = staging
    env["DBT_SCHEMA"] = args.schema
    if args.target == "local":
        env["WAREHOUSE_PATH"] = os.path.abspath(
            env.get("WAREHOUSE_PATH", os.path.join(PROJ, "warehouse")))
    elif not env.get("WAREHOUSE_PATH"):
        sys.exit("ERROR: --target onelake needs WAREHOUSE_PATH (abfss://.../Tables)")
    warehouse = env["WAREHOUSE_PATH"]

    if not args.skip_generate:
        gen = [sys.executable, os.path.join(HERE, "generate_data.py"),
               "--sf", str(args.sf), "--out",
               staging if not str(staging).startswith("abfss://") else args.staging]
        if args.force:
            gen.append("--force")
        print(">> generating data", flush=True)
        subprocess.run(gen, check=True, env=env)

    print(f">> connect {warehouse} (schema {args.schema})", flush=True)
    conn = connect(warehouse, args.schema)
    conn.con.execute("SET TimeZone='UTC'")

    print(">> init: Audit + DImessages", flush=True)
    conn.sql(audit_ddl(staging))
    conn.sql(DIMESSAGES_DDL)
    conn.refresh()
    n_audit = conn.sql("SELECT count(*) AS n FROM Audit").fetchone()[0]
    print(f"   Audit rows: {n_audit:,}; DImessages created empty", flush=True)
    if args.init_only:
        print(">> init-only done.", flush=True)
        return

    timings: dict[str, float] = {}
    for batch in BATCHES[:args.batches]:
        print(f"\n>> batch {batch}: dbt run (sequential project)", flush=True)
        t0 = time.perf_counter()
        dbt_run(batch, env)
        # See new tables before the between-batch validation reads them.
        conn.refresh()
        n_pcr = run_sql_file(conn, "batch_complete.sql", {"batch_id": batch})
        n_val = run_sql_file(conn, "batch_validation.sql", {"batch_id": batch})
        conn.refresh()
        timings[f"batch{batch}"] = time.perf_counter() - t0
        print(f"   batch {batch}: {timings[f'batch{batch}']:.1f}s "
              f"({n_pcr} phase-complete + {n_val} validation statement(s))", flush=True)

    if not args.skip_test:
        print("\n>> dbt test --select tag:sequential", flush=True)
        dbt_test(env)

    if not args.skip_audit:
        audit = os.path.join(PROJ, "tools", "run_sequential_audit.py")
        if os.path.exists(audit):
            print("\n>> audit: tools/run_sequential_audit.py", flush=True)
            t0 = time.perf_counter()
            rc = subprocess.run([sys.executable, audit], env=env).returncode
            timings["audit"] = time.perf_counter() - t0
        else:
            print("\n>> (skip: tools/run_sequential_audit.py not present yet)", flush=True)
            rc = 0
    else:
        rc = 0

    print("\n>> wall-clock")
    for k in ("batch1", "batch2", "batch3", "audit"):
        if k in timings:
            print(f"   {k:<8} {timings[k]:8.1f}s")

    if rc:
        sys.exit(f">> AUDIT FAILED (exit {rc}).")
    print(">> done.", flush=True)


if __name__ == "__main__":
    main()
