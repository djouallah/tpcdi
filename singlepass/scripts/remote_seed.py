"""Launch heavy TPC-DI phases on Fabric compute via duckrun's ``Workspace.run_python``.

The GitHub runner is 4 vCPUs, ~16 GiB RAM, ~14 GiB free disk. Two phases outgrow it:

* **Seed generation** (sf > 370): the per-table streamer only ever keeps one table on local
  disk, so the ceiling is the LARGEST SINGLE TABLE — at sf1000 (~1 TB warehouse) a single
  table no longer fits (DailyMarket alone is ~32 GB).
* **The Appendix-A audit** (sf >= 100): its heaviest check row-multiplies DimTrade × DimDate
  × DimBroker and already spilled ~79 GiB at sf100. The audit SQL is canonical and
  untouchable — the fix is bigger hardware, not a rewrite.

Both re-run THIS repo's own scripts (``--remote local``) inside a throwaway Fabric Python
notebook: ~135 GiB work disk, memory scaling with vCores, data-local to OneLake, tokens
self-acquired via the Fabric runtime. All the transport — payload shipping, live ``[remote]``
log streaming, result read-back, session-death retries with backoff, 0_temp parking and
notebook teardown — is duckrun's (``Workspace.run_python``), shared with its dbt
``RemoteRunner``, so transport fixes land once, upstream. The **local checkout is the
payload**: whatever code the runner has is exactly what executes remotely — no repo
re-download, no GITHUB_SHA plumbing. Seed generation additionally installs a portable JDK 8
through ``run_python``'s ``setup=`` hook (PDGF's Eclipse jar-in-jar loader breaks on 9+).

Used by ``stream_seed.py --remote auto`` (seed, remote for sf > 370) and
``tools/run_sequential_audit.py --remote auto`` (audit, remote for sf >= 100); direct use:

    WAREHOUSE_PATH=abfss://…/Tables python remote_seed.py --sf 1000 --prefix tpcdi/sf1000
"""
from __future__ import annotations

import argparse
import math
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
# scripts/ lives inside the dbt project dir (sequential/ or singlepass/), which lives at the
# repo root — the repo checkout IS the remote payload, and the entry point is project-relative.
PROJECT = os.path.basename(os.path.dirname(HERE))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))

# run_python `setup=` hook: portable JDK 8 into the notebook's work dir (no root needed) —
# PDGF's Eclipse jar-in-jar loader breaks on Java 9+. Runs in duckrun's harness namespace
# (say/tee/os/sys in scope, cwd = the extracted payload on the big work disk); the PATH export
# propagates into the script subprocess. A User-Agent is mandatory: Adoptium/GitHub 403 bare
# urllib requests.
_JDK8_SETUP = """\
import glob, shutil, tarfile, urllib.request
if not glob.glob('jdk/*/bin/java'):
    say('downloading portable JDK 8 (Adoptium) ...')
    _req = urllib.request.Request(
        'https://api.adoptium.net/v3/binary/latest/8/ga/linux/x64/jdk/hotspot/normal/eclipse',
        headers={'User-Agent': 'tpcdi-remote-seed/1'})
    with urllib.request.urlopen(_req) as _r, open('jdk8.tgz', 'wb') as _f:
        shutil.copyfileobj(_r, _f)
    with tarfile.open('jdk8.tgz') as _t:
        try:
            _t.extractall('jdk', filter='fully_trusted')  # keep symlinks + exec bits
        except TypeError:
            _t.extractall('jdk')
    os.remove('jdk8.tgz')
os.environ['PATH'] = os.path.abspath(glob.glob('jdk/*/bin')[0]) + os.pathsep + os.environ['PATH']
tee(['java', '-version'])
"""


def auto_cores(sf: int) -> int:
    """Same sizing as run.py's remote dbt — doubling per HALF decade: 8 vCores at sf100,
    16 from ~sf320, 32 at sf1000, 64 from ~sf3200 (Fabric's cap; valid sizes only)."""
    return min(64, 8 * 2 ** int(2 * math.log10(max(sf, 100) / 100)))


def _workspace_and_lakehouse(warehouse: str):
    """(workspace_guid, lakehouse_guid) from an ``abfss://…/Tables`` OneLake warehouse URL."""
    m = re.match(r"abfss://([^@]+)@[^/]+/([^/]+)/Tables$", warehouse.rstrip("/"))
    if not m:
        sys.exit(f"ERROR: remote execution needs an abfss://…/Tables warehouse; got {warehouse!r}")
    return m.group(1), m.group(2)


def _run_remote(label: str, name: str, entry: str, script_args: list, env: dict,
                warehouse: str, cores: int, pip: list, setup: str = None) -> None:
    """Ship the repo checkout and run one of its scripts on Fabric; exit non-zero on failure."""
    import duckrun

    ws_guid, lh_guid = _workspace_and_lakehouse(warehouse)
    ws = duckrun.workspace(ws_guid)
    print(f">> remote {label}: {entry} on Fabric compute (vCores={cores})", flush=True)
    res = ws.run_python(REPO_ROOT, entry=entry, name=name, lakehouse=lh_guid, cores=cores,
                        args=script_args, env=env, pip=pip, setup=setup)
    if not res.success:
        sys.exit(f">> remote {label} FAILED (exit {res.returncode}) — see the remote log above")
    print(f">> remote {label} SUCCEEDED", flush=True)


def launch(sf: int, prefix: str, warehouse: str, cores: int = 0, ref: str = "") -> None:
    """Remote SEED GENERATION: this project's stream_seed.py on Fabric (with JDK 8).
    ``ref`` is accepted for backward compatibility and ignored — the payload is the local
    checkout, so what the caller runs is exactly what executes remotely."""
    _run_remote(
        "seed generation", f"tpcdi-seed-sf{sf}",
        f"{PROJECT}/scripts/stream_seed.py",
        ["--sf", str(sf), "--prefix", prefix, "--warehouse", warehouse, "--remote", "local"],
        env={"WAREHOUSE_PATH": warehouse,
             "TPCDI_JVM_XMX": os.environ.get("TPCDI_JVM_XMX", "4g"),
             "TPCDI_GEN_TIMEOUT": os.environ.get("TPCDI_GEN_TIMEOUT", "10800")},
        warehouse=warehouse, cores=cores or auto_cores(sf),
        pip=["duckrun", "obstore"], setup=_JDK8_SETUP)


def launch_audit(sf: int, schema: str, warehouse: str, cores: int = 0, ref: str = "") -> None:
    """Remote AUDIT: this project's tools/run_sequential_audit.py on Fabric — the canonical
    Appendix-A SQL, unchanged, just running where the memory/disk is."""
    _run_remote(
        "audit", f"tpcdi-audit-sf{sf}",
        f"{PROJECT}/tools/run_sequential_audit.py",
        ["--warehouse", warehouse, "--schema", schema, "--remote", "local"],
        env={"WAREHOUSE_PATH": warehouse, "DBT_SCHEMA": schema, "TPCDI_SF": str(sf)},
        warehouse=warehouse, cores=cores or auto_cores(sf),
        pip=["duckrun"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    ap.add_argument("--prefix", default=os.environ.get("TPCDI_ONELAKE_PREFIX", "tpcdi"))
    ap.add_argument("--warehouse", default=os.environ.get("WAREHOUSE_PATH", ""))
    ap.add_argument("--cores", type=int, default=int(os.environ.get("TPCDI_REMOTE_CORES", "0")),
                    help="Fabric notebook vCores (0 = scale with --sf: 8@sf100 doubling per "
                         "half-decade, cap 64)")
    args = ap.parse_args()
    if not args.warehouse:
        sys.exit("ERROR: --warehouse (or WAREHOUSE_PATH) required")
    launch(args.sf, args.prefix, args.warehouse, cores=args.cores)


if __name__ == "__main__":
    main()
