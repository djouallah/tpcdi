"""Sequential TPC-DI audit — the COMPLETE Appendix-A automated_audit.sql.

Runs the full 130-check Appendix-A audit (ported verbatim to DuckDB in
``sequential/sql/automated_audit.sql``) against the finished sequential warehouse and the
per-batch ``DImessages`` log that ``run.py`` produces. Unlike the single-pass end-state
audit, NOTHING is skipped and there is NO WARN tier: every check is FAIL-fatal, so the run
exits nonzero if ANY check's Result is not 'OK'.

The audit SELECT reads, all by bare name (duckrun sets the schema on the connection):
  * the warehouse Delta tables (Dim*/Fact*/Financial/Prospect/reference tables),
  * ``Audit``      — the PDGF answer keys (run.py materialized it from Batch*/*_audit.csv),
  * ``DImessages`` — the validation/PCR/visibility/alert log written between batches.
All of these already exist as Delta tables when this runs, so the audit is pure read-only:
we connect through duckrun with ``read_only=True`` (same as scripts/run_queries.py) and run
the single big UNION ALL, which yields one (Test, Batch, Result, Description) row per check.

Local::
    WAREHOUSE_PATH=./warehouse DBT_SCHEMA=tpcdi3_seq python sequential/tools/run_sequential_audit.py
OneLake::
    WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \\
      DBT_SCHEMA=tpcdi3_seq ONELAKE_TOKEN=... python sequential/tools/run_sequential_audit.py

Each check prints one line: ``PASS/FAIL | test | batch | description``. Exit status is
nonzero iff any check FAILs.
"""
from __future__ import annotations

import argparse
import os
import sys

import duckrun

HERE = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(HERE)
AUDIT_SQL = os.path.join(PROJ, "sql", "automated_audit.sql")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default=os.environ.get("WAREHOUSE_PATH"),
                    help="root_path of the Delta warehouse (local dir or abfss://.../Tables)")
    ap.add_argument("--schema", default=os.environ.get("DBT_SCHEMA", "tpcdi_seq"))
    args = ap.parse_args()

    if not args.warehouse:
        sys.exit("ERROR: set --warehouse or WAREHOUSE_PATH (local dir or abfss://.../Tables)")

    storage_options = None
    if args.warehouse.startswith("abfss://"):
        token = os.environ.get("ONELAKE_TOKEN", "")
        if not token:
            sys.exit("ERROR: ONELAKE_TOKEN is empty — needed to query an abfss:// warehouse")
        storage_options = {"bearer_token": token}

    if not os.path.exists(AUDIT_SQL):
        sys.exit(f"ERROR: audit SQL not found at {AUDIT_SQL}")
    # utf-8-sig strips a UTF-8 BOM if present — a leading BOM would make DuckDB's parser
    # choke on the first token of the audit SELECT.
    with open(AUDIT_SQL, "r", encoding="utf-8-sig") as fh:
        sql = fh.read()

    conn = duckrun.connect(
        args.warehouse, storage_options=storage_options, schema=args.schema, read_only=True)
    conn.con.execute("SET TimeZone='UTC'")
    raw = conn.con  # bypass duckrun's Delta-DML classifier — the audit is a plain SELECT

    print(f"\n  warehouse: {args.warehouse} (schema {args.schema})")
    print(f"  audit:     {AUDIT_SQL}\n")

    try:
        rows = raw.execute(sql).fetchall()
    except Exception as e:
        sys.exit(f"ERROR: audit query failed to execute: {e}")

    # The classic audit returns (Test, Batch, Result, Description). A check passes iff its
    # Result is exactly 'OK'; anything else (the check's else-branch message) is a failure.
    print(f"  {'status':<6} {'test':<44}{'batch':>6}  detail")
    print("  " + "-" * 96)
    n_pass = n_fail = 0
    failures = []
    for row in rows:
        test, batch, result, description = row[0], row[1], row[2], row[3]
        ok = (result == "OK")
        status = "PASS" if ok else "FAIL"
        if ok:
            n_pass += 1
        else:
            n_fail += 1
            failures.append((test, batch, result, description))
        b = "" if batch is None else str(batch)
        detail = description if ok else f"{result}  ({description})"
        print(f"  {status:<6} {str(test):<44}{b:>6}  {detail}")
    print("  " + "-" * 96)
    print(f"  {len(rows)} checks: {n_pass} passed, {n_fail} failed")

    if not rows:
        sys.exit("\n  AUDIT PRODUCED NO ROWS — the audit query returned nothing; "
                 "the warehouse/DImessages may be empty.")
    if n_fail:
        print("\n  FAILURES:")
        for test, batch, result, description in failures:
            b = "" if batch is None else f" [batch {batch}]"
            print(f"    - {test}{b}: {result}  ({description})")
        _diagnostics(raw)
        sys.exit(f"\n  AUDIT FAILED — {n_fail} check(s) not OK.")
    print("\n  AUDIT PASSED — all 130 Appendix-A checks OK.")


def _diagnostics(raw) -> None:
    """Print expected-vs-actual numbers for the known-tricky checks (read-only against the
    existing Audit + DImessages, so a fast reuse run reveals them without a rebuild)."""
    print("\n  --- diagnostics ---")
    queries = [
        ("Audit datasets present (need 13: Batch, Dim*, Fact*, Financial, Generator, Prospect)",
         "SELECT dataset, count(*) AS rows FROM Audit GROUP BY dataset ORDER BY dataset"),
        ("DOB alerts  actual (DImessages) per batch",
         "SELECT BatchID, count(*) AS n FROM dimessages WHERE MessageType='Alert' "
         "AND MessageText='DOB out of range' GROUP BY BatchID ORDER BY BatchID"),
        ("DOB alerts  expected (Audit C_DOB_TO + C_DOB_TY) per batch",
         "SELECT BatchID, sum(Value) AS n FROM Audit WHERE DataSet='DimCustomer' "
         "AND Attribute IN ('C_DOB_TO','C_DOB_TY') GROUP BY BatchID ORDER BY BatchID"),
        ("Tier alerts actual (DImessages) per batch",
         "SELECT BatchID, count(*) AS n FROM dimessages WHERE MessageType='Alert' "
         "AND MessageText='Invalid customer tier' GROUP BY BatchID ORDER BY BatchID"),
        ("Tier alerts expected (Audit C_TIER_INV) per batch",
         "SELECT BatchID, sum(Value) AS n FROM Audit WHERE DataSet='DimCustomer' "
         "AND Attribute='C_TIER_INV' GROUP BY BatchID ORDER BY BatchID"),
        ("DimCustomer batchid distribution",
         "SELECT batchid, count(*) AS rows, count(*) FILTER (WHERE tier NOT IN (1,2,3) OR tier IS NULL) "
         "AS bad_tier FROM DimCustomer GROUP BY batchid ORDER BY batchid"),
    ]
    for label, sql in queries:
        try:
            rows = raw.execute(sql).fetchall()
            print(f"  {label}:")
            for r in rows:
                print(f"      {tuple(r)}")
        except Exception as e:
            print(f"  {label}: (query error: {str(e).splitlines()[0][:80]})")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
