"""Per-table seed manifests for resumable OneLake seed generation.

`stream_seed.py` streams the 16 PDGF tables to `Files/<prefix>/` one at a time. After a table's
files land, we write a tiny manifest at `Files/<prefix>/_manifest/<table>.json` recording every
file it produced and that file's byte size (plus the table's audit-summary contribution). On the
next run we list what is actually present in OneLake and mark a table **done** iff every file its
manifest claims is present *at a matching size* — so a re-run regenerates only the tables not
already fully and correctly uploaded, instead of all-or-nothing.

The manifest is only a claim; correctness comes from re-verifying presence+size against a live
listing (`obstore.list` gives `.path`/`.size`), so a partial/killed upload — in any order — is
never mistaken for done. PDGF is seed-deterministic, so an identical size is a strong correctness
signal (no need to re-hash multi-GB files).

OneLake only (abfss://). A local `--warehouse` (small-SF testing) skips all of this.
"""
from __future__ import annotations

import json
import os
import re
import sys

from duckrun import auth


def _files_store_root(warehouse: str) -> str:
    """Map the warehouse ``abfss://{ws}@{host}/{lh}/Tables`` URL to the lakehouse root
    ``abfss://{ws}@{host}/{lh}/`` that obstore's ``AzureStore`` takes; per-file object paths
    under it are then ``Files/<prefix>/<rel>`` (Tables holds Delta, Files holds loose seed files)."""
    w = warehouse.rstrip("/")
    m = re.match(r"(abfss://[^@]+@[^/]+/[^/]+)/Tables$", w)
    if not m:
        sys.exit(f"ERROR: WAREHOUSE_PATH is not a OneLake '…/Tables' URL: {warehouse}")
    return m.group(1) + "/"


def mint_token() -> str:
    """A FRESH OneLake storage token (GitHub OIDC → ONELAKE_TOKEN env fallback)."""
    token = auth.refresh_storage_token() or os.environ.get("ONELAKE_TOKEN", "")
    if not token:
        sys.exit("ERROR: could not mint a OneLake token (need AZURE_CLIENT_ID/TENANT_ID or ONELAKE_TOKEN)")
    return token


def connect(warehouse: str, token: str | None = None):
    """A obstore ``AzureStore`` rooted at the lakehouse. Mints a fresh token when not given one, so
    callers can re-connect between long per-table steps that outlast the ~1h token life."""
    from obstore.store import AzureStore
    return AzureStore.from_url(_files_store_root(warehouse), bearer_token=token or mint_token())


def delete_object(warehouse: str, object_path: str, token: str) -> None:
    """Per-file DELETE via the OneLake **DFS REST** endpoint (ignores 404). We need this because
    ``obstore.delete`` uses a bulk-batch API OneLake rejects (400), and OneLake refuses a multipart
    PUT onto an existing blob — especially one written by another writer (e.g. legacy duckrun
    ``conn.copy``) — with 409 BlobOperationNotSupported. So to replace a file we DFS-delete it, then
    obstore-put a fresh path."""
    import urllib.error
    import urllib.request
    m = re.match(r"abfss://([^@]+)@([^/]+)/([^/]+)/Tables$", warehouse.rstrip("/"))
    if not m:
        sys.exit(f"ERROR: WAREHOUSE_PATH is not a OneLake '…/Tables' URL: {warehouse}")
    ws, host, lh = m.groups()
    req = urllib.request.Request(f"https://{host}/{ws}/{lh}/{object_path}", method="DELETE",
                                 headers={"Authorization": f"Bearer {token}"})
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code != 404:  # 404 = already gone
            raise


def _base(prefix: str) -> str:
    return f"Files/{prefix.strip('/')}"


def manifest_object(prefix: str, table: str) -> str:
    return f"{_base(prefix)}/_manifest/{table}.json"


def list_present(store, prefix: str) -> dict:
    """{object_path: size_bytes} for everything under Files/<prefix>/ — one recursive listing."""
    import obstore
    present = {}
    for batch in obstore.list(store, _base(prefix) + "/"):
        for meta in batch:
            present[meta["path"]] = meta["size"]
    return present


def read_manifest(store, prefix: str, table: str):
    """The table's manifest dict, or None if absent/unreadable."""
    import obstore
    try:
        data = obstore.get(store, manifest_object(prefix, table)).bytes()
    except Exception:  # noqa: BLE001 — missing object
        return None
    try:
        return json.loads(bytes(data))
    except Exception:  # noqa: BLE001 — corrupt manifest → treat as absent
        return None


def write_manifest(warehouse: str, prefix: str, table: str, files: list, summary: dict) -> None:
    """Record a completed table: ``{"table", "files":[{"path","size"}], "summary":{check:val}}``.
    ``files`` paths are relative to Files/<prefix> (e.g. ``Batch1/WatchHistory.txt``). DFS-delete any
    prior manifest (idempotent re-run) then obstore-put — self-contained (mints its own token)."""
    import obstore
    token = mint_token()
    obj = manifest_object(prefix, table)
    delete_object(warehouse, obj, token)
    body = json.dumps({"table": table, "files": files, "summary": summary}, indent=2).encode("utf-8")
    obstore.put(connect(warehouse, token), obj, body)


def table_done(present: dict, prefix: str, manifest) -> bool:
    """True iff the manifest exists and every file it claims is present at a matching byte size."""
    if not manifest:
        return False
    files = manifest.get("files") or []
    if not files:
        return False
    base = _base(prefix)
    return all(present.get(f"{base}/{f['path']}") == f["size"] for f in files)


def plan_tables(store, prefix: str, tables):
    """Return (todo, done, present, manifests). A table is `done` iff its manifest verifies against
    the live listing (every claimed file present at its recorded byte size); everything else is
    `todo`. No manifest ⇒ regenerate — so a seed written before manifests existed, or one left
    partially uploaded, is rebuilt from scratch on the next run and gains manifests going forward.
    That first rebuild overwrites any stale files harmlessly (obstore.put overwrites)."""
    present = list_present(store, prefix)
    manifests = {t: read_manifest(store, prefix, t) for t in tables}
    done = [t for t in tables if table_done(present, prefix, manifests[t])]
    todo = [t for t in tables if t not in done]
    return todo, done, present, manifests
