"""Generate the TPC-DI seed one PDGF table at a time and stream each straight to OneLake —
so local disk only ever holds a single table, and arbitrarily large scale factors fit a small
runner.

**Resumable:** before generating, we ask `seed_manifest.plan_tables` which tables are already in
OneLake with every file present at its recorded byte size, and skip those — so a re-run after a
partial failure regenerates only what's missing (and a full cache hit does no generation at all,
not even cloning the datagen toolkit). Each completed table writes a manifest that the next run
verifies. See seed_manifest.py.

This is **stage 1** only: generate each table locally and copy it to OneLake byte-for-byte, with a
per-table manifest recording each file's byte size so the copy is verifiable/resumable. It does NOT
compute any audit aggregates — the source→target reconciliation (stage 2) is the audit's job and
reads the seed straight from OneLake.

The loop, per to-generate table:

    PDGF -start <table>  ->  local staging
      -> (split CustomerMgmt.xml into chunks)
      -> mint a FRESH OneLake token   (survives the ~1h token life across a long build)
      -> obstore.put staging -> Files/<prefix>   (multipart stream; accumulates; never wipes)
      -> write per-table manifest (files + byte sizes)
      -> delete local staging
      -> next table

PDGF is seed-deterministic, so per-table output is byte-identical to one full run (verified by
diffing TradeSource/DailyMarket against the all-at-once seed). Files upload via obstore
(``obstore.put`` streams each file as a multipart upload — no per-file size limit, unlike duckrun's
``conn.copy`` which reads a whole file into one ~4 GiB-capped DuckDB BLOB), landed at their relative
path without wiping the prefix, so the final OneLake seed is exactly what the all-at-once upload
would have produced.

Env: WAREHOUSE_PATH (abfss://…/Tables), AZURE_CLIENT_ID / AZURE_TENANT_ID (to mint tokens via
GitHub OIDC). Works against a LOCAL warehouse dir too (no token needed) for testing.

Usage:
    python stream_seed.py --sf 370 --prefix tpcdi/sf370            # OneLake (WAREHOUSE_PATH env)
    python stream_seed.py --sf 3 --warehouse C:/tmp/wh --prefix tpcdi/sf3   # local, for testing
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys

import duckrun

# generate_data (PDGF driver) lives beside this script.
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import generate_data as gd  # noqa: E402
import seed_manifest as sm  # noqa: E402

# PDGF table names, small → large (each deleted after upload, so order only affects the log).
# Together these produce every seed file the dbt models read.
TABLES = [
    "StatusType", "TaxRate", "Industry", "TradeType", "Date", "Time", "BatchDate", "HR",
    "FINWIRE", "CustomerMgmt", "Customer", "Account", "Prospect", "WatchHistory",
    "DailyMarket", "TradeSource",
]


def _staged_files(staging: str) -> list:
    """[{"path": <rel-with-forward-slashes>, "size": bytes}] for every file under `staging` —
    the manifest of what this table's upload will land under Files/<prefix>/."""
    out = []
    for dirpath, _dirs, names in os.walk(staging):
        for name in names:
            local_path = os.path.join(dirpath, name)
            rel = os.path.relpath(local_path, staging).replace("\\", "/")
            out.append({"path": rel, "size": os.path.getsize(local_path)})
    return out


def _upload(warehouse: str, staging: str, prefix: str, present: dict | None = None) -> None:
    """Stream every file under ``staging`` to ``Files/<prefix>/<rel>``, preserving the tree and never
    wiping the prefix. OneLake goes via **obstore** — ``obstore.put`` streams straight from the file
    handle as a multipart upload, so there is NO per-file size limit. (duckrun's ``conn.copy`` reads
    each file whole into one DuckDB BLOB, which caps at ~4 GiB — WatchHistory.txt alone blows that
    past ~sf290.) A local warehouse dir (offline testing) still uses ``conn.copy``. A FRESH OneLake
    token is minted per call so a long per-table loop survives the ~1h token life.

    ``present`` (object_path -> size, from the pre-run listing) lets us delete-before-put only the
    files that already exist: OneLake rejects a multipart PUT onto an existing blob (409), so a stale
    file from a prior/partial run must be cleared first; fresh paths just put."""
    if not warehouse.startswith("abfss://"):
        duckrun.connect(warehouse, read_only=False).copy(staging, prefix, overwrite=True)
        return
    import obstore
    token = sm.mint_token()
    store = sm.connect(warehouse, token)
    present = present or {}
    pfx = prefix.strip("/")
    n = 0
    for dirpath, _dirs, names in os.walk(staging):
        for name in names:
            local_path = os.path.join(dirpath, name)
            rel = os.path.relpath(local_path, staging).replace("\\", "/")
            object_path = f"Files/{pfx}/{rel}"
            gb = os.path.getsize(local_path) / 1e9
            if object_path in present:
                # OneLake 409s a multipart PUT onto an existing blob — DFS-delete first (obstore.delete
                # is broken on OneLake). Fresh paths skip the delete.
                sm.delete_object(warehouse, object_path, token)
            print(f"  obstore put → {object_path} ({gb:.2f} GB, multipart)", flush=True)
            with open(local_path, "rb") as fh:
                obstore.put(store, object_path, fh)
            n += 1
    print(f"  uploaded {n} file(s) to Files/{pfx}/", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, default=int(os.environ.get("TPCDI_SF", "3")))
    ap.add_argument("--prefix", default=os.environ.get("TPCDI_ONELAKE_PREFIX", "tpcdi"))
    ap.add_argument("--warehouse", default=os.environ.get("WAREHOUSE_PATH", ""))
    ap.add_argument("--staging", default=os.environ.get("TPCDI_STAGING", "./staging"))
    ap.add_argument("--work", default=os.environ.get("TPCDI_WORK", "./_tpcdi_work"))
    ap.add_argument("--tables", nargs="*", default=None, help="subset of TABLES (default: all)")
    args = ap.parse_args()

    if args.sf < 3:
        sys.exit("ERROR: TPC-DI minimum scale factor is 3")
    if not args.warehouse:
        sys.exit("ERROR: --warehouse (or WAREHOUSE_PATH) required")

    staging = os.path.abspath(args.staging)
    chunk = int(os.environ.get("TPCDI_CM_CHUNK", "20000"))
    tables = args.tables or TABLES
    onelake = args.warehouse.startswith("abfss://")

    # Per-table resume (OneLake only): skip tables already present with matching byte sizes, so a
    # re-run after a partial failure regenerates only what's missing. A local warehouse (small-SF
    # testing) has no manifests — regenerate everything.
    done, present = [], {}
    if onelake:
        todo, done, present, _manifests = sm.plan_tables(sm.connect(args.warehouse), args.prefix, tables)
        print(f"\nseed resume: {len(done)}/{len(tables)} table(s) already present, "
              f"{len(todo)} to generate", flush=True)
        if done:
            print(f"  present: {', '.join(done)}", flush=True)
        if todo:
            print(f"  to-gen : {', '.join(todo)}", flush=True)
    else:
        todo = list(tables)

    if todo:
        gd._require_java()
        datagen = gd._fetch_datagen(args.work)
        for i, tbl in enumerate(todo, 1):
            print(f"\n=== [{i}/{len(todo)}] {tbl} ===", flush=True)
            shutil.rmtree(staging, ignore_errors=True)
            os.makedirs(staging, exist_ok=True)
            gd._run_digen(datagen, args.sf, staging, tables=[tbl])
            if tbl == "CustomerMgmt":
                gd._split_customermgmt(os.path.join(staging, "Batch1"), chunk)
            files = _staged_files(staging)
            _upload(args.warehouse, staging, args.prefix, present)
            if onelake:
                # Commit the table: next run verifies presence+size from the manifest and skips it.
                sm.write_manifest(args.warehouse, args.prefix, tbl, files, {})
                print(f"  manifest: {tbl} ({len(files)} file(s))", flush=True)

    shutil.rmtree(staging, ignore_errors=True)
    print(f"\n  seed ready -> Files/{args.prefix}/ ; generated {len(todo)}, reused {len(done)}", flush=True)


if __name__ == "__main__":
    main()
