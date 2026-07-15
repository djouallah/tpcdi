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
         "FROM (SELECT DISTINCT customerid FROM DimCustomer WHERE batchid=1 AND tier IS NULL) dc "
         "JOIN stg_customermgmt s ON s.customerid=dc.customerid "
         "GROUP BY dc.customerid HAVING max(s.tier) IN (1,2,3) LIMIT 12"),
        ("Of those NULL-tier-but-valid-stg customers, how many have the tier on their NEW action",
         "SELECT count(*) FILTER (WHERE new_tier IN (1,2,3)) AS tier_on_new, "
         "count(*) FILTER (WHERE new_tier IS NULL) AS tier_only_later FROM ("
         "  SELECT dc.customerid, max(s.tier) FILTER (WHERE s.actiontype='NEW') AS new_tier "
         "  FROM DimCustomer dc JOIN stg_customermgmt s ON s.customerid=dc.customerid "
         "  WHERE dc.batchid=1 AND dc.tier IS NULL "
         "  GROUP BY dc.customerid HAVING max(s.tier) IN (1,2,3)) x"),
        # --- Tier-alert reconciliation (Step 1 & Step 3 of the runbook) ---
        # Step 1: rule out duplicate answer-key ingestion. The tier check sums Audit.Value,
        # so a row loaded twice inflates 'expected'. Expect exactly ONE row per batch.
        ("Step1  Audit C_TIER_INV rows per batch (must be 1 row/batch; >1 = dup ingestion)",
         "SELECT batchid, count(*) AS rows, sum(value) AS total FROM Audit "
         "WHERE dataset='DimCustomer' AND attribute='C_TIER_INV' GROUP BY batchid ORDER BY batchid"),
        # Step 3(a): collapse victims — NEW lacks tier (counted by the key) but a same-day
        # valid UPDCUST erased the NULL version via WHERE effectivedate<enddate, so no bad
        # version survives -> 1 expected, 0 alerts. This is the b1 gap's prime candidate.
        ("Step3a collapse victims b1 (NEW no-tier, no surviving bad version = expected-not-alerted)",
         "SELECT count(*) AS victims FROM (SELECT DISTINCT customerid FROM stg_customermgmt "
         "  WHERE actiontype='NEW' AND tier IS NULL) s "
         "WHERE NOT EXISTS (SELECT 1 FROM DimCustomer d WHERE d.customerid=s.customerid "
         "  AND d.batchid=1 AND (d.tier IS NULL OR d.tier NOT IN (1,2,3)))"),
        ("Step3a  ^ list of collapse-victim customerids (up to 20)",
         "SELECT s.customerid FROM (SELECT DISTINCT customerid FROM stg_customermgmt "
         "  WHERE actiontype='NEW' AND tier IS NULL) s "
         "WHERE NOT EXISTS (SELECT 1 FROM DimCustomer d WHERE d.customerid=s.customerid "
         "  AND d.batchid=1 AND (d.tier IS NULL OR d.tier NOT IN (1,2,3))) ORDER BY 1 LIMIT 20"),
        # Step 3(b): multi-event customers — 2+ counted events deduped to 1 alert by the
        # QUALIFY. Each extra event = +1 expected with no matching alert (upper bound: can't
        # see PDGF changedThisUpdate()).
        ("Step3b multi-event customers b1 (2+ counted tier events -> deduped to 1 alert)",
         "SELECT customerid, count(*) AS events FROM stg_customermgmt "
         "WHERE (actiontype='NEW'     AND (tier IS NULL OR tier NOT IN (1,2,3))) "
         "   OR (actiontype='UPDCUST' AND tier IS NOT NULL AND tier NOT IN (1,2,3)) "
         "GROUP BY customerid HAVING count(*) > 1 ORDER BY events DESC LIMIT 20"),
        ("Step3b  ^ total extra events = sum(events-1) over multi-event customers",
         "SELECT coalesce(sum(events-1),0) AS extra_events FROM ("
         "  SELECT customerid, count(*) AS events FROM stg_customermgmt "
         "  WHERE (actiontype='NEW'     AND (tier IS NULL OR tier NOT IN (1,2,3))) "
         "     OR (actiontype='UPDCUST' AND tier IS NOT NULL AND tier NOT IN (1,2,3)) "
         "  GROUP BY customerid HAVING count(*) > 1)"),
        # Step 3 first-suspect: does try_cast(actionts AS timestamp) keep sub-second precision?
        # If precision is lost, same-day lead() ordering (which version collapses) can diverge.
        ("Step3  stg update_ts rows with sub-second precision lost (0 = truncated to whole seconds)",
         "SELECT count(*) FILTER (WHERE update_ts != date_trunc('second', update_ts)) AS sub_second, "
         "count(*) FILTER (WHERE update_ts != date_trunc('day', update_ts)) AS sub_day, "
         "count(*) AS total FROM stg_customermgmt"),
        # --- DimCustomer row-count reconciliation (check #29 'Too few rows' at batch 1) ---
        # actual batch-N versions vs the answer-key minimum C_NEW+C_INACT+C_UPDCUST-C_ID_HIST.
        # A negative (actual-expected) says our SCD2 collapse drops more versions than C_ID_HIST.
        ("Row count b1: actual vs expected_min (C_NEW+C_INACT+C_UPDCUST-C_ID_HIST) + components",
         "WITH a AS (SELECT batchid, "
         "  sum(value) FILTER (WHERE attribute='C_NEW')     AS c_new, "
         "  sum(value) FILTER (WHERE attribute='C_INACT')   AS c_inact, "
         "  sum(value) FILTER (WHERE attribute='C_UPDCUST') AS c_updcust, "
         "  sum(value) FILTER (WHERE attribute='C_ID_HIST') AS c_id_hist "
         "  FROM Audit WHERE dataset='DimCustomer' GROUP BY batchid), "
         "act AS (SELECT batchid, count(*) AS actual FROM DimCustomer GROUP BY batchid) "
         "SELECT a.batchid, act.actual, "
         "  coalesce(c_new,0)+coalesce(c_inact,0)+coalesce(c_updcust,0)-coalesce(c_id_hist,0) AS expected_min, "
         "  act.actual - (coalesce(c_new,0)+coalesce(c_inact,0)+coalesce(c_updcust,0)-coalesce(c_id_hist,0)) AS gap, "
         "  c_new, c_inact, c_updcust, c_id_hist "
         "FROM a JOIN act USING(batchid) ORDER BY a.batchid"),
        # --- FactWatches reconciliation (checks #83 row count, #85 active watches) ---
        # #83 wants (RowCount[b]-RowCount[b-1]) == WH_ACTIVE[b]; #85 wants
        # RowCount[b]+Inactive[b] == running-sum(WH_RECORDS). LHS < RHS => watches dropped.
        ("FactWatches DImessages actual: Row count / Inactive watches per batch",
         "SELECT batchid, messagetext, sum(cast(messagedata AS bigint)) AS n "
         "FROM DImessages WHERE messagesource='FactWatches' AND messagetype='Validation' "
         "AND messagetext IN ('Row count','Inactive watches') GROUP BY batchid, messagetext "
         "ORDER BY batchid, messagetext"),
        ("FactWatches answer keys: WH_ACTIVE / WH_RECORDS per batch (+ running WH_RECORDS)",
         "SELECT batchid, "
         "  sum(value) FILTER (WHERE attribute='WH_ACTIVE')  AS wh_active, "
         "  sum(value) FILTER (WHERE attribute='WH_RECORDS') AS wh_records, "
         "  sum(sum(value) FILTER (WHERE attribute='WH_RECORDS')) OVER (ORDER BY batchid) AS wh_records_running "
         "FROM Audit WHERE dataset='FactWatches' AND batchid IN (1,2,3) GROUP BY batchid ORDER BY batchid"),
        ("FactWatches actual table: new rows / running total / rows-with-dateremoved per batch",
         "SELECT batchid, count(*) AS rows_this_batch, "
         "  sum(count(*)) OVER (ORDER BY batchid) AS running_total, "
         "  count(*) FILTER (WHERE sk_dateid_dateremoved IS NOT NULL) AS removed_this_batch "
         "FROM FactWatches GROUP BY batchid ORDER BY batchid"),
        # --- DOB alerts = 0 in batches 2/3 (check #41) ---
        # audit_alerts recomputes 'DOB out of range' from the final DimCustomer grouped by the
        # version's batchid. Batch 1 matches (319) but 2/3 emit 0 vs 6 expected. Are there any
        # batch-2/3 versions that even trip the rule, and what dobs do those versions carry?
        ("DOB rule on batch-2/3 DimCustomer versions (too_old / too_young / null_dob)",
         "SELECT dc.batchid, count(*) AS versions, "
         "  count(*) FILTER (WHERE dob <= bd.batchdate - INTERVAL 100 YEAR) AS too_old, "
         "  count(*) FILTER (WHERE dob > bd.batchdate) AS too_young, "
         "  count(*) FILTER (WHERE dob IS NULL) AS null_dob "
         "FROM DimCustomer dc JOIN BatchDate bd USING(batchid) "
         "WHERE dc.batchid IN (2,3) GROUP BY dc.batchid ORDER BY dc.batchid"),
        ("Batch-2/3 dob range vs batchdate (are extreme dobs present in these versions at all?)",
         "SELECT dc.batchid, min(dc.dob) AS min_dob, max(dc.dob) AS max_dob, "
         "  any_value(bd.batchdate) AS batchdate "
         "FROM DimCustomer dc JOIN BatchDate bd USING(batchid) "
         "WHERE dc.batchid IN (2,3) GROUP BY dc.batchid ORDER BY dc.batchid"),
        # --- Root-cause probes: is the divergence webbed extraction, source truth, or
        #     whole-second ordering ties? (the DimCustomer transform is a verbatim port of the
        #     reference, so a mismatch must originate in stg_customermgmt content or the CDC path)
        # Q1: did webbed silently drop actions, or is the stg action set complete? Compare the
        # stg per-actiontype counts to the PDGF answer-key action tallies.
        ("Q1 stg_customermgmt action count by actiontype",
         "SELECT actiontype, count(*) AS n FROM stg_customermgmt GROUP BY actiontype ORDER BY actiontype"),
        ("Q1 Audit answer-key action tallies (C_NEW/ADDACCT/UPDACCT/UPDCUST/CLOSEACCT/INACT/ID_HIST)",
         "SELECT dataset, attribute, sum(value) AS n FROM Audit "
         "WHERE attribute IN ('C_NEW','C_ADDACCT','C_UPDACCT','C_UPDCUST','C_CLOSEACCT','C_INACT','C_ID_HIST') "
         "GROUP BY dataset, attribute ORDER BY dataset, attribute"),
        # Q2: extraction-bug vs source-truth. For null-tier customers, dump per-action whether the
        # OTHER Customer attributes are present on the NEW action. If NEW carries tax/gndr but
        # tier=-/dob=- -> the source NEW genuinely lacks tier (webbed is fine). If NEW is all '-'
        # -> webbed dropped the whole Customer parse on NEW.
        ("Q2 null-tier customers: per-action attribute presence (tier/dob/tax/gndr) by update_ts (10)",
         "SELECT s.customerid, string_agg("
         "  s.actiontype || ':t=' || coalesce(cast(s.tier AS varchar),'-')"
         "  || ',dob=' || coalesce(cast(s.dob AS varchar),'-')"
         "  || ',tax=' || (CASE WHEN s.taxid IS NULL THEN '-' ELSE 'Y' END)"
         "  || ',g=' || coalesce(s.gender,'-'), ' | ' ORDER BY s.update_ts) AS actions "
         "FROM (SELECT DISTINCT customerid FROM DimCustomer WHERE batchid=1 AND tier IS NULL) dc "
         "JOIN stg_customermgmt s ON s.customerid=dc.customerid "
         "GROUP BY s.customerid HAVING max(s.tier) IN (1,2,3) LIMIT 10"),
        # Q3: whole-second ordering ties. try_cast preserves sub-second precision in DuckDB, so
        # sub_second=0 means the SOURCE ActionTS is whole-second. That makes same-second ties
        # possible, and lead()/last_value() over `order by update_ts` can then collapse a
        # DIFFERENT version than Spark. Count the collapse-relevant (NEW/INACT/UPDCUST) ties.
        ("Q3 same-second ordering ties among NEW/INACT/UPDCUST actions (tie pairs, extra tied rows)",
         "SELECT count(*) AS tie_pairs, coalesce(sum(n-1),0) AS extra_tied_rows FROM ("
         "  SELECT customerid, update_ts, count(*) AS n FROM stg_customermgmt "
         "  WHERE actiontype IN ('NEW','INACT','UPDCUST') "
         "  GROUP BY customerid, update_ts HAVING count(*) > 1)"),
        # Q4: FactWatches cascade proxy. WatchHistory.txt is not a warehouse table, so instead
        # count customers whose first surviving DimCustomer version starts AFTER their NEW action
        # date -> their NEW version was collapsed, so any watch placed on the NEW day falls before
        # the earliest [effectivedate,enddate) range and is dropped by the historical join.
        ("Q4 customers whose 1st DimCustomer version starts after their NEW date (watch-drop cause)",
         "SELECT count(*) AS missing_early_coverage FROM ("
         "  SELECT dc.customerid, min(dc.effectivedate) AS first_ver, "
         "    (SELECT min(date(s.update_ts)) FROM stg_customermgmt s "
         "     WHERE s.customerid=dc.customerid AND s.actiontype='NEW') AS new_date "
         "  FROM DimCustomer dc WHERE dc.batchid=1 GROUP BY dc.customerid) "
         "WHERE new_date IS NOT NULL AND first_ver > new_date"),
        # --- Round 2 probes (all warehouse-only): pin DOB b2/3, tier +35, DimCustomer -2 ---
        # DOB b2/3 (0 vs 6 expected). Our audit_alerts groups the DOB rule by the VERSION's
        # batchid, so batch-2/3 only sees the ~500 new versions (none extreme). The reference
        # answer key counts customers who CROSS 100y during a batch. Test: per batch, count
        # extreme-DOB customers three ways -- (ver) scoped to versions with that batchid,
        # (cur_calyr/cur_bday) over the version CURRENT as of that batchdate under the
        # calendar-year rule the reference uses vs our birthday-accurate rule. If cur_calyr is
        # ~319/325/331 then the per-batch increments 319/6/6 match the key -> the fix is to
        # count newly-extreme current customers per batch, not version-batchid.
        ("D1 DOB per batch: version-scoped vs current-as-of-batchdate (calyr vs birthday rule)",
         "SELECT bd.batchid, "
         "  (SELECT count(DISTINCT dc.customerid) FROM DimCustomer dc WHERE dc.batchid=bd.batchid "
         "     AND (datediff('year', dc.dob, bd.batchdate) >= 100 OR dc.dob > bd.batchdate)) AS ver_calyr, "
         "  (SELECT count(*) FROM DimCustomer dc WHERE dc.effectivedate <= bd.batchdate AND bd.batchdate < dc.enddate "
         "     AND (datediff('year', dc.dob, bd.batchdate) >= 100 OR dc.dob > bd.batchdate)) AS cur_calyr, "
         "  (SELECT count(*) FROM DimCustomer dc WHERE dc.effectivedate <= bd.batchdate AND bd.batchdate < dc.enddate "
         "     AND (dc.dob <= bd.batchdate - INTERVAL 100 YEAR OR dc.dob > bd.batchdate)) AS cur_bday "
         "FROM BatchDate bd WHERE bd.batchid IN (1,2,3) ORDER BY bd.batchid"),
        # Tier +35 (7844 vs 7809). audit_alerts flags `tier NOT IN (1,2,3) OR tier IS NULL`
        # (verbatim from the reference). Does dropping the NULL clause match the key 7809?
        ("D2 tier b1 alert count: with vs without the 'OR tier IS NULL' clause (key=7809)",
         "SELECT count(DISTINCT customerid) FILTER (WHERE tier NOT IN (1,2,3) OR tier IS NULL) AS with_null, "
         "  count(DISTINCT customerid) FILTER (WHERE tier NOT IN (1,2,3)) AS without_null "
         "FROM DimCustomer WHERE batchid=1"),
        # DimCustomer b1 -2 (217898 vs 217900). With C_ID_HIST=0 every NEW/INACT/UPDCUST action
        # should yield a surviving version; -2 = two same-day collapses (effectivedate=enddate).
        # List the culprits: customers whose surviving version count is below their action count.
        ("D3 DimCustomer b1 collapse victims: customerid, action_count, version_count (sum of gaps=2)",
         "SELECT s.customerid, s.acts, coalesce(d.vers,0) AS vers FROM "
         "  (SELECT customerid, count(*) AS acts FROM stg_customermgmt "
         "   WHERE actiontype IN ('NEW','INACT','UPDCUST') GROUP BY customerid) s "
         "  LEFT JOIN (SELECT customerid, count(*) AS vers FROM DimCustomer WHERE batchid=1 GROUP BY customerid) d "
         "  USING(customerid) WHERE coalesce(d.vers,0) < s.acts ORDER BY (s.acts-coalesce(d.vers,0)) DESC LIMIT 20"),
        # --- Round 3: DOB model + FactWatches join-drop localization ---
        # D4 DOB gross new crossers per batch: customers whose version current-as-of-batchdate is
        # extreme (birthday rule) but were NOT extreme in the immediately-prior batch. Batch 1 =
        # all extreme (319). If this yields 319/6/6 it exactly matches the key and is the fix.
        ("D4 DOB gross new crossers per batch (birthday rule, current-version-at-batchdate)",
         "WITH ext AS (SELECT bd.batchid, dc.customerid FROM BatchDate bd "
         "  JOIN DimCustomer dc ON dc.effectivedate <= bd.batchdate AND bd.batchdate < dc.enddate "
         "  WHERE bd.batchid IN (1,2,3) AND (dc.dob <= bd.batchdate - INTERVAL 100 YEAR OR dc.dob > bd.batchdate)) "
         "SELECT batchid, count(*) AS gross_new FROM ext e2 "
         "WHERE NOT EXISTS (SELECT 1 FROM ext e1 WHERE e1.customerid=e2.customerid AND e1.batchid=e2.batchid-1) "
         "GROUP BY batchid ORDER BY batchid"),
    ]

    # D5 FactWatches join-drop localization: reads Batch1/WatchHistory.txt straight from the
    # OneLake seed (TPCDI_DIR) and left-joins the dims to see whether the ~12.3K un-placed
    # historical watches fail the customer or the security join, and whether it's a missing
    # entity or a date-range-coverage gap. Skipped when TPCDI_DIR is unset (local runs).
    _tpcdi_dir = os.environ.get("TPCDI_DIR", "")
    if _tpcdi_dir:
        _wh = f"{_tpcdi_dir}/Batch1/WatchHistory.txt"
        queries.append((
            "D5 FactWatches Batch1 watch pairs vs customer/security join coverage",
            "WITH w AS (SELECT w_c_id, w_s_symb, date(min(w_dts)) AS dateplaced FROM read_csv('"
            + _wh + "', delim='|', header=false, quote='', escape='', nullstr='', null_padding=true, "
            "columns={'w_c_id':'BIGINT','w_s_symb':'VARCHAR','w_dts':'TIMESTAMP','w_action':'VARCHAR'}) "
            "GROUP BY w_c_id, w_s_symb) "
            "SELECT count(*) AS pairs, "
            "count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM DimCustomer c WHERE c.customerid=w.w_c_id)) AS no_cust_entity, "
            "count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM DimSecurity s WHERE s.symbol=w.w_s_symb)) AS no_sec_entity, "
            "count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM DimCustomer c WHERE c.customerid=w.w_c_id "
            "  AND w.dateplaced >= c.effectivedate AND w.dateplaced < c.enddate)) AS no_cust_range, "
            "count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM DimSecurity s WHERE s.symbol=w.w_s_symb "
            "  AND w.dateplaced >= s.effectivedate AND w.dateplaced < s.enddate)) AS no_sec_range "
            "FROM w"))

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
