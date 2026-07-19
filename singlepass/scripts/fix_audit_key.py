"""Repair the PDGF answer key's C_DOB_TO / C_DOB_TY (djouallah/tpcdi#1).

PDGF's `Customer-Audit.xml` counts the incremental DOB alerts against a batch date shifted
+CMUpdateLastID days (a hard-zeroed `updateIDOffset`), so `Batch{2,3}/Customer_audit.csv`
carries phantom C_DOB_TO/C_DOB_TY values the emitted `Customer.txt` cannot reproduce — at any
scale factor with a dob inside the ~14-month phantom window (sf10: 1, sf100: 6 per batch), a
faithful ETL can never pass the `DimCustomer age range alerts` audit check.

The generator itself cannot be fixed: PDGF ships with `HARD_CODED_SCHEMA=true` — the TPC-DI
schema, generation config, and audit counter templates are compiled into pdgf.jar with tamper
detection, and the `pdgf/config/*.xml` files on disk are unread reference copies (verified:
editing or even deleting Customer-Audit.xml changes nothing, and `-load` is ignored — DIGen's
own source comments it out as "not being recognized"). So the key is repaired *after*
generation instead, by recounting the two attributes from the seed's own data with the
generator's documented rule:

    C_DOB_TO :  c_dob < batch_date - 36,525 days   (PDGF's ONE_CENTURY_IN_MS, Julian century)
    C_DOB_TY :  c_dob > batch_date                 (batch date of the batch being audited)

Only those two value fields in Customer_audit.csv change — every other byte of the seed stays
byte-identical vanilla DIGen output. Used two ways:

  * imported by the seed pipeline (`stream_seed.py` / `generate_data.py`) to repair a freshly
    generated Customer table before upload — `repair_seed_root(staging)`;
  * as a CLI one-off against an existing OneLake seed (dry-run by default, --apply to write):
        python fix_audit_key.py --warehouse abfss://…/Tables --prefix tpcdi/sf10 --apply

If the CSV's byte size changes (it doesn't for single-digit values), the OneLake `Customer`
table manifest is refreshed so `stream_seed.py`'s presence+size resume check keeps passing.
"""
from __future__ import annotations

import argparse
import datetime
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# PDGF's pdgf.util.Constants.ONE_CENTURY_IN_MS expressed in days (Julian century).
CENTURY_DAYS = 36525
# FIRST_BATCH_DATE_START in tpc-di-schema.xml; batch N's date is FIRST + (N-1) days. Used when
# the seed tree has no Batch{b}/BatchDate.txt (a Customer-only staging dir); when the file is
# present it is authoritative.
FIRST_BATCH_DATE = datetime.date(2017, 7, 7)
BATCHES = (2, 3)
DOB_COL = 10  # 0-based position of C_DOB in pipe-delimited Customer.txt


class _Seed:
    """Read/write the handful of seed files we touch, OneLake (abfss) or a local dir."""

    def __init__(self, warehouse: str = "", prefix: str = "", seed_root: str = ""):
        self.onelake = not seed_root
        if self.onelake:
            import seed_manifest as sm
            self.sm, self.warehouse, self.prefix = sm, warehouse, prefix.strip("/")
            self.token = sm.mint_token()
            self.store = sm.connect(warehouse, self.token)
        else:
            self.root = seed_root

    def _object(self, rel: str) -> str:
        return f"Files/{self.prefix}/{rel}"

    def exists(self, rel: str) -> bool:
        if self.onelake:
            import obstore
            try:
                obstore.head(self.store, self._object(rel))
                return True
            except Exception:  # noqa: BLE001 — missing object
                return False
        return os.path.isfile(os.path.join(self.root, rel))

    def read_text(self, rel: str) -> str:
        if self.onelake:
            import obstore
            return bytes(obstore.get(self.store, self._object(rel)).bytes()).decode("utf-8")
        with open(os.path.join(self.root, rel), encoding="utf-8", newline="") as fh:
            return fh.read()

    def write_text(self, rel: str, text: str) -> None:
        data = text.encode("utf-8")
        if self.onelake:
            import obstore
            # OneLake 409s a PUT onto an existing blob — DFS-delete first (see stream_seed).
            self.sm.delete_object(self.warehouse, self._object(rel), self.token)
            obstore.put(self.store, self._object(rel), data)
        else:
            with open(os.path.join(self.root, rel), "w", encoding="utf-8", newline="") as fh:
                fh.write(text)

    def refresh_manifest_size(self, rel: str, size: int) -> None:
        """Update the Customer manifest's recorded size for `rel` (OneLake only) so the
        seed-resume presence+size check keeps passing after a rewrite."""
        if not self.onelake:
            return
        manifest = self.sm.read_manifest(self.store, self.prefix, "Customer")
        if not manifest:
            print("    (no Customer manifest — nothing to refresh)", flush=True)
            return
        changed = False
        for f in manifest.get("files", []):
            if f["path"] == rel and f["size"] != size:
                f["size"], changed = size, True
        if changed:
            self.sm.write_manifest(self.warehouse, self.prefix, "Customer",
                                   manifest["files"], manifest.get("summary", {}))
            print(f"    manifest refreshed: {rel} -> {size} bytes", flush=True)


def _batch_date(seed: _Seed, batch: int) -> datetime.date:
    rel = f"Batch{batch}/BatchDate.txt"
    if seed.exists(rel):
        txt = seed.read_text(rel).strip()
        return datetime.date.fromisoformat(txt.split("|")[0].strip())
    return FIRST_BATCH_DATE + datetime.timedelta(days=batch - 1)


def _recount(seed: _Seed, batch: int) -> dict:
    """C_DOB_TO / C_DOB_TY for Batch<batch>/Customer.txt under the documented rule."""
    bdate = _batch_date(seed, batch)
    cutoff = bdate - datetime.timedelta(days=CENTURY_DAYS)
    to = ty = rows = 0
    for line in seed.read_text(f"Batch{batch}/Customer.txt").splitlines():
        if not line:
            continue
        rows += 1
        cols = line.split("|")
        dob = cols[DOB_COL].strip() if len(cols) > DOB_COL else ""
        if not dob:
            continue
        d = datetime.date.fromisoformat(dob)
        if d < cutoff:
            to += 1
        elif d > bdate:
            ty += 1
    return {"C_DOB_TO": to, "C_DOB_TY": ty, "rows": rows,
            "batch_date": bdate, "cutoff": cutoff}


def _rewrite(csv: str, batch: int, counts: dict) -> tuple[str, list]:
    """Replace only the two DOB value fields; everything else byte-identical."""
    changes = []
    for attr in ("C_DOB_TO", "C_DOB_TY"):
        pat = re.compile(rf"(DimCustomer,{batch},,{attr},)(-?\d+)(,)")
        m = pat.search(csv)
        if not m:
            sys.exit(f"ERROR: {attr} row for batch {batch} not found in Customer_audit.csv")
        old, new = int(m.group(2)), counts[attr]
        if old != new:
            csv = pat.sub(rf"\g<1>{new}\g<3>", csv, count=1)
        changes.append((attr, old, new))
    return csv, changes


def repair(seed: _Seed, apply: bool) -> bool:
    """Recount + rewrite both incremental batches. Returns True if phantoms were found."""
    dirty = False
    for batch in BATCHES:
        if not seed.exists(f"Batch{batch}/Customer.txt"):
            print(f"  Batch{batch}: no Customer.txt — skipped", flush=True)
            continue
        counts = _recount(seed, batch)
        rel = f"Batch{batch}/Customer_audit.csv"
        csv = seed.read_text(rel)
        fixed, changes = _rewrite(csv, batch, counts)
        print(f"  Batch{batch}: {counts['rows']} records, batch_date={counts['batch_date']}, "
              f"too-old cutoff={counts['cutoff']}", flush=True)
        for attr, old, new in changes:
            mark = "  (phantom)" if old != new else ""
            print(f"    {attr}: {old} -> {new}{mark}", flush=True)
        if fixed != csv:
            dirty = True
            if apply:
                seed.write_text(rel, fixed)
                seed.refresh_manifest_size(rel, len(fixed.encode("utf-8")))
                print(f"    wrote {rel}", flush=True)
    return dirty


def repair_seed_root(root: str) -> None:
    """Pipeline hook: repair a freshly generated local seed tree in place (tpcdi#1)."""
    print("  audit-key repair (tpcdi#1):", flush=True)
    repair(_Seed(seed_root=root), apply=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default=os.environ.get("WAREHOUSE_PATH", ""),
                    help="abfss://…/Tables (OneLake seed)")
    ap.add_argument("--prefix", default="tpcdi", help="seed prefix under Files/, e.g. tpcdi/sf10")
    ap.add_argument("--seed-root", default="", help="local seed dir (contains Batch2/, Batch3/)")
    ap.add_argument("--apply", action="store_true", help="write the corrected values")
    args = ap.parse_args()

    if not args.seed_root and not args.warehouse:
        sys.exit("ERROR: --warehouse (or WAREHOUSE_PATH) or --seed-root required")

    seed = _Seed(args.warehouse, args.prefix, args.seed_root)
    where = args.seed_root or f"{args.prefix} @ {args.warehouse}"
    print(f"seed: {where}  ({'APPLY' if args.apply else 'dry-run'})", flush=True)
    dirty = repair(seed, apply=args.apply)

    if dirty and not args.apply:
        print("\nphantom values found — re-run with --apply to fix them", flush=True)
    elif not dirty:
        print("\nkey already consistent — nothing to do", flush=True)


if __name__ == "__main__":
    main()
