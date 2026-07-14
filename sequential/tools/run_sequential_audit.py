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
import re
import sys
import time

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

    # Run the audit ONE query per check (not one giant UNION ALL) so progress is visible
    # live — each line prints as its check finishes, and a slow/hanging check is obvious by
    # name instead of a silent multi-minute stall. Every check returns (Test, Batch, Result,
    # Description); a check passes iff Result is exactly 'OK'.
    checks = _split_checks(sql)
    print(f"  running {len(checks)} checks (one query each):\n")
    print(f"  {'#':>4}  {'status':<6} {'test':<42}{'batch':>6}  detail")
    print("  " + "-" * 98)
    n_pass = n_fail = n_err = 0
    total = 0
    failures = []
    for i, csql in enumerate(checks, 1):
        try:
            crows = _run_one(raw, csql)
        except Exception as e:
            n_err += 1
            line = str(e).splitlines()[0]
            failures.append((f"check #{i}", None, "ERROR", line[:80]))
            print(f"  {i:>4}  {'ERROR':<6} {'(query failed)':<42}{'':>6}  {line[:56]}", flush=True)
            continue
        for row in crows:
            total += 1
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
            print(f"  {i:>4}  {status:<6} {str(test):<42}{b:>6}  {detail}", flush=True)
    print("  " + "-" * 98)
    print(f"  {len(checks)} checks, {total} results: {n_pass} passed, {n_fail} failed"
          + (f", {n_err} errored" if n_err else ""))

    if total == 0 and n_err == 0:
        sys.exit("\n  AUDIT PRODUCED NO ROWS — the warehouse/DImessages may be empty.")
    if n_fail or n_err:
        print("\n  FAILURES:")
        for test, batch, result, description in failures:
            b = "" if batch is None else f" [batch {batch}]"
            print(f"    - {test}{b}: {result}  ({description})")
        _diagnostics(raw)
        sys.exit(f"\n  AUDIT FAILED — {n_fail + n_err} check(s) not OK.")
    print("\n  AUDIT PASSED — all 130 Appendix-A checks OK.")


def _split_checks(sql: str):
    """Split the ported audit (one `select * from ( <check> union all <check> … ) q`) into
    its individual check SELECTs. Every `union all` in this audit is top-level (none are
    nested inside a check's subqueries), so a plain split on `union all` is exact."""
    # Strip line comments first — the header prose itself contains "select * from ( … ) q",
    # which would otherwise fool the wrapper detection. (No `--` appears inside a string
    # literal in this audit.)
    body = "\n".join((ln if ln.find("--") < 0 else ln[:ln.find("--")]) for ln in sql.splitlines())
    m = re.search(r"select\s+\*\s+from\s*\(", body, re.IGNORECASE)
    inner = body[m.end():] if m else body
    inner = re.sub(r"\)\s*q\s*;?\s*$", "", inner.strip())      # drop the closing ') q'
    parts = re.split(r"\n\s*union all\s*\n", inner, flags=re.IGNORECASE)
    return [p.strip() for p in parts if p.strip()]


def _run_one(raw, csql: str):
    """Execute one check, retrying a couple of times on a transient object-store blip."""
    for attempt in range(1, 4):
        try:
            return raw.execute(csql).fetchall()
        except Exception as e:
            transient = any(s in str(e) for s in (
                "ObjectStoreError", "error sending request", "HTTP error",
                "timed out", "connection", "Connection", "reset"))
            if transient and attempt < 3:
                time.sleep(5)
                continue
            raise


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
        ("C_DOB_TO vs C_DOB_TY answer keys (batch 1)",
         "SELECT attribute, sum(Value) AS n FROM Audit WHERE DataSet='DimCustomer' "
         "AND BatchID=1 AND attribute IN ('C_DOB_TO','C_DOB_TY') GROUP BY attribute"),
        # Is a bad DOB genuinely in the source, or introduced by the historical forward-fill?
        # Show DOB-year histogram of batch-1 flagged customers, and compare each flagged
        # customer's DimCustomer dob vs its raw stg_customermgmt dob(s).
        ("DOB-year histogram of batch-1 flagged DISTINCT customers",
         "SELECT year(dob) AS dob_year, count(DISTINCT customerid) AS customers "
         "FROM DimCustomer dc JOIN BatchDate bd USING (batchid) "
         "WHERE batchid=1 AND (datediff('year', dob, batchdate) >= 100 OR dob > batchdate) "
         "GROUP BY year(dob) ORDER BY customers DESC LIMIT 12"),
        ("Flagged customers: DimCustomer dob  vs  raw stg_customermgmt dob(s) (10 samples)",
         "SELECT dc.customerid, dc.dob AS dimcust_dob, "
         "  string_agg(DISTINCT cast(s.dob AS varchar), ',') AS stg_dobs, "
         "  string_agg(DISTINCT s.actiontype, ',') AS actions "
         "FROM DimCustomer dc JOIN BatchDate bd USING (batchid) "
         "LEFT JOIN stg_customermgmt s ON s.customerid = dc.customerid "
         "WHERE dc.batchid=1 AND (datediff('year', dc.dob, bd.batchdate) >= 100 OR dc.dob > bd.batchdate) "
         "GROUP BY dc.customerid, dc.dob ORDER BY dc.customerid LIMIT 10"),
        ("Tier: distinct customers with ANY bad-tier version, batch 1 (producer logic)",
         "SELECT count(DISTINCT customerid) AS flagged FROM DimCustomer "
         "WHERE batchid=1 AND (tier NOT IN (1,2,3) OR tier IS NULL)"),
        ("Tier value distribution among batch-1 bad-tier customers",
         "SELECT coalesce(cast(tier AS varchar), 'NULL') AS tier, count(DISTINCT customerid) AS customers "
         "FROM DimCustomer WHERE batchid=1 AND (tier NOT IN (1,2,3) OR tier IS NULL) "
         "GROUP BY tier ORDER BY customers DESC"),
        # Customers whose ONLY bad-tier version is non-current (a bad tier that appears in an
        # intermediate SCD2 version but not the latest) — a candidate for the 252-vs-251 gap.
        ("DOB recompute with the birthday-accurate rule (should be 8 for batch 1)",
         "SELECT count(DISTINCT customerid) AS n FROM DimCustomer dc JOIN BatchDate bd USING (batchid) "
         "WHERE batchid=1 AND (dob <= batchdate - INTERVAL 100 YEAR OR dob > batchdate)"),
        # The tier gap is one extra NULL-tier customer: is that NULL genuine (source never
        # gave a tier) or did the load drop a valid tier the raw XML actually has?
        ("NULL-tier DimCustomer customers (b1) who DO have a valid tier in raw stg_customermgmt",
         "SELECT count(DISTINCT dc.customerid) AS n FROM DimCustomer dc WHERE dc.batchid=1 AND dc.tier IS NULL "
         "AND EXISTS (SELECT 1 FROM stg_customermgmt s WHERE s.customerid=dc.customerid AND s.tier IN (1,2,3))"),
        ("  ^ those customers: per-action tier in raw stg (does the NEW action carry a tier?)",
         "SELECT dc.customerid, "
         "  string_agg(s.actiontype || '=' || coalesce(cast(s.tier AS varchar),'-'), ', ' ORDER BY s.update_ts) AS action_tiers "
         "FROM DimCustomer dc JOIN stg_customermgmt s ON s.customerid=dc.customerid "
         "WHERE dc.batchid=1 AND dc.tier IS NULL "
         "GROUP BY dc.customerid HAVING max(s.tier) IN (1,2,3) LIMIT 12"),
        ("Of those NULL-tier-but-valid-stg customers, how many have the tier on their NEW action",
         "SELECT count(*) FILTER (WHERE new_tier IN (1,2,3)) AS tier_on_new, "
         "count(*) FILTER (WHERE new_tier IS NULL) AS tier_only_later FROM ("
         "  SELECT dc.customerid, max(s.tier) FILTER (WHERE s.actiontype='NEW') AS new_tier "
         "  FROM DimCustomer dc JOIN stg_customermgmt s ON s.customerid=dc.customerid "
         "  WHERE dc.batchid=1 AND dc.tier IS NULL "
         "  GROUP BY dc.customerid HAVING max(s.tier) IN (1,2,3)) x"),
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
