"""Run the analytical queries in queries/ against the TPC-DI warehouse.

Read-only by design: connects through duckrun with ``read_only=True`` so a query run
can never write the warehouse. It is therefore safe to run against the live OneLake
tables completely independently of — and even concurrently with — the ETL pipeline
(Delta reads are snapshot-isolated). This is the query side of the benchmark; the
warehouse itself is built by the ETL workflow (see run_benchmark.py / tpc_di.yml).

Each file in the queries directory is one plain SELECT. Tables are referenced by bare
double-quoted name (e.g. "DimCustomer"); the schema is set on the connection.

Local:
    WAREHOUSE_PATH=./warehouse python scripts/run_queries.py
OneLake:
    WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \\
        ONELAKE_TOKEN=... python scripts/run_queries.py
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
import time

import duckrun

HERE = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(HERE)


def _cell(v) -> str:
    s = "" if v is None else str(v)
    return s if len(s) <= 28 else s[:25] + "..."


def _mdcell(v) -> str:
    """A value formatted for a Markdown table cell (numbers get thousands separators)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        return f"{v:,.2f}"
    return str(v).replace("|", "\\|")


def _description(sql: str) -> str:
    """The leading `-- ` comment block of a query file, as a one-line description."""
    out = []
    for line in sql.splitlines():
        s = line.strip()
        if s.startswith("--"):
            out.append(s.lstrip("-").strip())
        elif s:
            break
    return " ".join(out)


def _write_step_summary(path, warehouse, schema, results, total_ms, preview):
    """Render a Markdown report to GitHub's step-summary page (or any file)."""
    passed = sum(1 for r in results if r["ok"])
    failed = len(results) - passed
    lines = [
        "# 📊 TPC-DI — Analytical Query Results",
        "",
        f"Read-only run over the built star schema — **{len(results)} queries**, "
        f"**{passed} passed**, **{failed} failed**, **{total_ms / 1000:.1f}s** total.",
        "",
        f"- **Warehouse:** `{warehouse}`",
        f"- **Schema:** `{schema}`",
        "",
        "| # | Query | Rows | Time (ms) | Status |",
        "|--:|:------|-----:|----------:|:------:|",
    ]
    for i, r in enumerate(results, 1):
        rows = f"{r['rows']:,}" if r["ok"] else "—"
        status = "✅" if r["ok"] else "❌"
        lines.append(f"| {i} | `{r['name']}` | {rows} | {r['ms']:.0f} | {status} |")
    lines.append("")

    for r in results:
        title = f"{r['name']} — {r['rows']:,} rows" if r["ok"] else f"{r['name']} — FAILED"
        lines += ["<details>", f"<summary>{title}</summary>", ""]
        if r["description"]:
            lines += [f"_{r['description']}_", ""]
        if not r["ok"]:
            lines += ["```", r["error"], "```", ""]
        elif r["cols"] and r["preview"]:
            lines.append("| " + " | ".join(r["cols"]) + " |")
            lines.append("|" + "|".join("---" for _ in r["cols"]) + "|")
            for row in r["preview"]:
                lines.append("| " + " | ".join(_mdcell(v) for v in row) + " |")
            if r["rows"] > preview:
                lines.append("")
                lines.append(f"_…{r['rows'] - preview:,} more row(s)._")
            lines.append("")
        lines += ["</details>", ""]

    with open(path, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default=os.environ.get("WAREHOUSE_PATH"),
                    help="root_path of the Delta warehouse (local dir or abfss://.../Tables)")
    ap.add_argument("--schema", default=os.environ.get("DBT_SCHEMA", "tpcdi"))
    ap.add_argument("--queries", default=os.path.join(PROJ, "queries"),
                    help="directory of .sql files to run (one SELECT each)")
    ap.add_argument("--preview", type=int, default=5,
                    help="rows to print (and put in the summary) per query (0 = counts/timing only)")
    ap.add_argument("--summary-md", default=None,
                    help="write a Markdown report here (defaults to $GITHUB_STEP_SUMMARY in CI)")
    args = ap.parse_args()

    if not args.warehouse:
        sys.exit("ERROR: set --warehouse or WAREHOUSE_PATH (local dir or abfss://.../Tables)")

    storage_options = None
    if args.warehouse.startswith("abfss://"):
        token = os.environ.get("ONELAKE_TOKEN", "")
        if not token:
            sys.exit("ERROR: ONELAKE_TOKEN is empty — needed to query an abfss:// warehouse")
        storage_options = {"bearer_token": token}

    files = sorted(glob.glob(os.path.join(args.queries, "*.sql")))
    if not files:
        sys.exit(f"ERROR: no .sql files found in {args.queries}")

    # read_only=True is the isolation guarantee: this connection cannot write anything.
    conn = duckrun.connect(
        args.warehouse, storage_options=storage_options, schema=args.schema, read_only=True)

    print(f"\n  warehouse: {args.warehouse} (schema {args.schema})")
    print(f"  queries:   {args.queries}  ({len(files)} file(s))\n")
    print(f"  {'query':<40}{'rows':>10}{'ms':>10}  status")
    print("  " + "-" * 72)

    results = []
    failures = []
    total0 = time.perf_counter()
    for path in files:
        name = os.path.basename(path)
        with open(path, "r", encoding="utf-8") as fh:
            sql = fh.read()
        rec = {"name": name, "description": _description(sql),
               "ok": False, "rows": 0, "ms": 0.0, "cols": [], "preview": [], "error": ""}
        t0 = time.perf_counter()
        try:
            rel = conn.sql(sql)
            cols = list(rel.columns)
            rows = rel.fetchall()
            rec.update(ok=True, rows=len(rows), ms=(time.perf_counter() - t0) * 1000,
                       cols=cols, preview=rows[:args.preview])
            print(f"  {name:<40}{len(rows):>10,}{rec['ms']:>10.0f}  ok")
            if args.preview and rows:
                print("      " + " | ".join(cols))
                for r in rows[:args.preview]:
                    print("      " + " | ".join(_cell(v) for v in r))
        except Exception as e:  # noqa: BLE001
            rec.update(ms=(time.perf_counter() - t0) * 1000, error=str(e).strip())
            print(f"  {name:<40}{'-':>10}{rec['ms']:>10.0f}  FAIL: {rec['error'].splitlines()[0]}")
            failures.append(name)
        results.append(rec)

    total_ms = (time.perf_counter() - total0) * 1000

    # Render a Markdown report to GitHub's run summary page (auto-set in every CI step),
    # or to --summary-md when given locally.
    summary_path = args.summary_md or os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        _write_step_summary(summary_path, args.warehouse, args.schema, results, total_ms, args.preview)
        print(f"\n  wrote summary -> {summary_path}")

    print()
    if failures:
        sys.exit(f"  {len(failures)} query(ies) failed: {', '.join(failures)}")
    print(f"  all {len(files)} query(ies) ran.")


if __name__ == "__main__":
    main()
