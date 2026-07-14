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

Reuses the same duckrun connection/env handling as the single-pass runner
(``WAREHOUSE_PATH``, ``ONELAKE_TOKEN``, ``DBT_SCHEMA``).

Local::

    WAREHOUSE_PATH=./warehouse TPCDI_DIR=./staging DBT_SCHEMA=tpcdi3_seq \\
        python sequential/scripts/run.py --sf 3

OneLake::

    WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \\
      TPCDI_DIR=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Files/tpcdi/sf3 \\
      ONELAKE_TOKEN=... DBT_SCHEMA=tpcdi3_seq \\
      python sequential/scripts/run.py --sf 3 --target onelake
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time

import duckrun

HERE = os.path.dirname(os.path.abspath(__file__))
# The sequential/ folder IS a complete, self-contained dbt project (its own
# dbt_project.yml / profiles.yml / macros / models / scripts / sql / tools). Nothing is
# shared with the single-pass project except the raw seed data.
SEQ_PROJ = os.path.dirname(HERE)      # the sequential/ dbt project dir
PROJ = SEQ_PROJ
SQLDIR = os.path.join(SEQ_PROJ, "sql")

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

    Loads the positional PDGF answer-key layout (DataSet, BatchID, Date, Attribute, Value,
    DValue) and drops any header/junk row via ``try_cast(BatchID) IS NOT NULL``, into a
    persistent Delta table — the sequential batch_validation and the full audit both read it.
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
-- Drop ONLY the header row (DataSet,BatchID,...); keep every real answer-key row including
-- the batch-less 'Generator' / 'Batch' meta rows (which have an empty BatchID) — the
-- 'Audit table sources' check needs all 13 DataSets present, not just the per-batch ones.
WHERE lower(trim(column0)) NOT IN ('dataset', '')
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
    # utf-8-sig strips a UTF-8 BOM if present — a leading BOM would keep duckrun's
    # `^\s*INSERT` router from recognizing the statement as a Delta write.
    with open(path, "r", encoding="utf-8-sig") as fh:
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


def warehouse_built(conn) -> bool:
    """True if a prior full 3-batch load already finished — the reuse gate.

    The sentinel is a batch-3 Phase Complete Record in DImessages, which only exists once
    the whole sequence (batches 1-3 + end-state) has run. Any error (the schema is empty /
    DImessages doesn't exist yet) means 'not built' -> do a full build.
    """
    try:
        row = conn.sql("SELECT count(*) AS n FROM DImessages "
                       "WHERE MessageType = 'PCR' AND BatchID = 3").fetchone()
        return bool(row and row[0])
    except Exception:
        return False


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
    ap.add_argument("--rebuild", action="store_true",
                    help="force a full rebuild even if the warehouse already exists. By "
                         "default, if a prior full 3-batch load already finished (a batch-3 "
                         "Phase Complete Record is in DImessages) the load + dbt test are "
                         "SKIPPED and the existing warehouse is reused — so re-running only "
                         "re-audits, without a ~15-min rebuild.")
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

    # The Audit answer-key table is static CSV data, independent of the warehouse build, so
    # (re)build it ALWAYS — even on reuse — so an audit_ddl / answer-key fix takes effect
    # without a full rebuild.
    print(">> Audit answer keys", flush=True)
    conn.sql(audit_ddl(staging))
    conn.refresh()
    n_audit = conn.sql("SELECT count(*) AS n FROM Audit").fetchone()[0]
    print(f"   Audit rows: {n_audit:,}", flush=True)

    # Reuse gate (on by default): if a prior full 3-batch load already finished, skip the
    # whole build (data load + DImessages + dbt test) and go straight to the audit against
    # the existing warehouse — re-running then just re-audits, no ~15-min rebuild. Pass
    # --rebuild to force. A full 3-batch run is the only one that yields a valid Appendix-A
    # audit (its DImessages checks need batches 0..3), so reuse implies full_run.
    full_run = args.batches == 3
    timings: dict[str, float] = {}
    reuse = warehouse_built(conn) and not args.rebuild

    if reuse:
        full_run = True
        print(">> warehouse already built (batch-3 PCR in DImessages) — reusing it; "
              "skipping data load + DImessages + dbt test. Pass --rebuild to force.",
              flush=True)
    else:
        print(">> init: DImessages", flush=True)
        conn.sql(DIMESSAGES_DDL)
        conn.refresh()
        print("   DImessages created empty", flush=True)
        if args.init_only:
            print(">> init-only done.", flush=True)
            return

        # Batch-0 initial-condition checkpoint (empty-DW PCR + 24 zero Validation rows) —
        # the audit requires DImessages to record the pre-Batch1 state. Precedes batch 1.
        n_init = run_sql_file(conn, "batch_initial.sql", {})
        conn.refresh()
        print(f"   batch 0: initial-condition checkpoint ({n_init} statement(s))", flush=True)

        for batch in BATCHES[:args.batches]:
            print(f"\n>> batch {batch}: dbt run (sequential project)", flush=True)
            t0 = time.perf_counter()
            dbt_run(batch, env)
            # See new tables before the between-batch validation reads them.
            conn.refresh()
            n_pcr = run_sql_file(conn, "batch_complete.sql", {"batch_id": batch})
            n_val = run_sql_file(conn, "batch_validation.sql", {"batch_id": batch})
            conn.refresh()
            # Data Visibility snapshot #1 after the historical load; #2 runs at end-of-run.
            # The audit asserts row counts are non-decreasing from snapshot 1 to snapshot 2.
            if batch == 1:
                run_sql_file(conn, "visibility_1.sql", {"batch_id": batch})
                conn.refresh()
                print("   visibility_1 snapshot written", flush=True)
            timings[f"batch{batch}"] = time.perf_counter() - t0
            print(f"   batch {batch}: {timings[f'batch{batch}']:.1f}s "
                  f"({n_pcr} phase-complete + {n_val} validation statement(s))", flush=True)

        if full_run:
            print("\n>> end-state: visibility_2 snapshot + audit_alerts", flush=True)
            run_sql_file(conn, "visibility_2.sql", {"batch_id": args.batches})
            run_sql_file(conn, "audit_alerts.sql", {})
            conn.refresh()

        if not args.skip_test:
            print("\n>> dbt test", flush=True)
            dbt_test(env)

    if not args.skip_audit and full_run:
        audit = os.path.join(PROJ, "tools", "run_sequential_audit.py")
        if os.path.exists(audit):
            print("\n>> audit: tools/run_sequential_audit.py (full Appendix-A)", flush=True)
            t0 = time.perf_counter()
            rc = subprocess.run([sys.executable, audit], env=env).returncode
            timings["audit"] = time.perf_counter() - t0
        else:
            print("\n>> (skip: tools/run_sequential_audit.py not present)", flush=True)
            rc = 0
    elif not args.skip_audit:
        print("\n>> (skip audit: needs a full 3-batch run — use --batches 3)", flush=True)
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
