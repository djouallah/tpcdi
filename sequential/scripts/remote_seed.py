"""Generate the TPC-DI seed ON Fabric compute — for scale factors the GitHub runner can't hold.

The per-table streamer (stream_seed.py) only ever keeps one table on local disk, so the seed's
scale-factor ceiling is the LARGEST SINGLE TABLE — and the hosted runner has ~14 GiB free
(measured ceiling ≈ sf370). At sf1000 (~1 TB warehouse) a single table no longer fits, so the
whole generate→upload loop must run where the disk is: a Fabric Python notebook has a ~135 GiB
local work disk and is data-local to OneLake.

This launcher builds a small Fabric PYTHON notebook that:
  * sizes its compute via ``%%configure`` (vCores scale with SF, same doubling formula as the
    remote dbt runner);
  * installs a portable JDK 8 into the work dir (PDGF's Eclipse jar-in-jar loader breaks on
    Java 9+; no root needed — Adoptium tarball);
  * downloads THIS repo at the launching commit (``GITHUB_SHA``) via codeload (no git needed);
  * runs the project's own ``stream_seed.py --remote local`` there — the identical per-table
    generate → fix_audit_key → obstore-upload → manifest loop, resumable exactly as on the
    runner. Tokens are self-acquired via notebookutils (first in duckrun's auth chain).

stream_seed runs as a SUBPROCESS of the notebook kernel, so the freshly pip-installed
duckdb/deltalake load in a clean interpreter — the Fabric stale-native-binary problem
(preinstalled versions already imported) never arises and no restartPython() is needed.

Deployed with ``overwrite=True`` and run via duckrun's workspace API. The notebook is KEPT
after the run (name: ``tpcdi_seed_sf<N>``) so its output stays inspectable in the Fabric UI;
the next launch overwrites it. A failed generation fails the Fabric job, which raises here.

Used by ``stream_seed.py --remote auto`` (remote for sf > 370); direct use:

    WAREHOUSE_PATH=abfss://…/Tables python remote_seed.py --sf 1000 --prefix tpcdi/sf1000
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
# scripts/ lives inside the dbt project dir (sequential/ or singlepass/) — the remote notebook
# runs the SAME project's stream_seed.py out of the repo download.
PROJECT = os.path.basename(os.path.dirname(HERE))
REPO_TARBALL = "https://codeload.github.com/djouallah/tpcdi/tar.gz/{ref}"
JDK8_URL = ("https://api.adoptium.net/v3/binary/latest/8/ga/linux/x64/jdk/hotspot/"
            "normal/eclipse")


def auto_cores(sf: int) -> int:
    """Same doubling-per-decade sizing as run.py's remote dbt: 8 vCores at sf100, 16 at
    sf1000, 32 at sf10000, capped at Fabric's largest size 64 (valid sizes only)."""
    return min(64, 8 * 2 ** int(math.log10(max(sf, 100) / 100)))


# Cell/notebook shapes copied from duckrun.fabric_remote — the metadata is load-bearing:
# kernel_info/kernelspec "jupyter" + microsoft.language_group "jupyter_python" is what makes
# Fabric create a PYTHON notebook (anything else yields a Spark notebook).
def _code_cell(src: str) -> dict:
    return {"cell_type": "code", "source": src.splitlines(keepends=True),
            "metadata": {"microsoft": {"language": "python", "language_group": "jupyter_python"}},
            "execution_count": None, "outputs": []}


def _notebook(cells: list) -> dict:
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "cells": cells,
        "metadata": {
            "kernelspec": {"name": "jupyter", "language": "Jupyter", "display_name": "Jupyter"},
            "language_info": {"name": "python"},
            "microsoft": {"language": "python", "language_group": "jupyter_python"},
            "kernel_info": {"name": "jupyter", "jupyter_kernel_name": "python3.12"},
            "dependencies": {"lakehouse": {}},
        },
    }


def build_seed_notebook(sf: int, prefix: str, warehouse: str, ref: str, cores: int,
                        jvm_xmx: str, gen_timeout: str) -> dict:
    """The generation notebook: JDK 8 + repo download + stream_seed, all under the ~135 GiB
    /home/trusted-service-user/work disk (the container's / and /tmp are a cramped overlay)."""
    cfg = {"sf": sf, "prefix": prefix, "warehouse": warehouse, "ref": ref,
           "project": PROJECT, "jvm_xmx": jvm_xmx, "gen_timeout": gen_timeout,
           "repo_url": REPO_TARBALL.format(ref=ref), "jdk_url": JDK8_URL}
    work = f"""\
import glob, os, shutil, subprocess, sys, tarfile, urllib.request
CFG = {json.dumps(cfg)}
WORK = '/home/trusted-service-user/work/tpcdi_seed'
os.makedirs(WORK, exist_ok=True)
os.chdir(WORK)

def untar(src, dst):
    with tarfile.open(src) as t:
        try:
            t.extractall(dst, filter='fully_trusted')  # keep symlinks + exec bits (JDK)
        except TypeError:
            t.extractall(dst)

subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', 'duckrun', 'obstore'], check=True)

if not glob.glob('jdk/*/bin/java'):
    print('downloading portable JDK 8 (Adoptium) ...', flush=True)
    urllib.request.urlretrieve(CFG['jdk_url'], 'jdk8.tgz')
    untar('jdk8.tgz', 'jdk')
    os.remove('jdk8.tgz')
os.environ['PATH'] = os.path.abspath(glob.glob('jdk/*/bin')[0]) + os.pathsep + os.environ['PATH']
subprocess.run(['java', '-version'], check=True)

print('fetching djouallah/tpcdi @ ' + CFG['ref'], flush=True)
urllib.request.urlretrieve(CFG['repo_url'], 'repo.tgz')
shutil.rmtree('repo', ignore_errors=True)
untar('repo.tgz', 'repo')
os.remove('repo.tgz')
stream = glob.glob('repo/*/' + CFG['project'] + '/scripts/stream_seed.py')[0]

env = dict(os.environ)
env.update({{'WAREHOUSE_PATH': CFG['warehouse'],
             'TPCDI_STAGING': WORK + '/staging',
             'TPCDI_WORK': WORK + '/work',
             'TPCDI_JVM_XMX': CFG['jvm_xmx'],
             'TPCDI_GEN_TIMEOUT': CFG['gen_timeout']}})
print('generating seed sf%s -> Files/%s' % (CFG['sf'], CFG['prefix']), flush=True)
subprocess.run([sys.executable, stream, '--sf', str(CFG['sf']), '--prefix', CFG['prefix'],
                '--warehouse', CFG['warehouse'], '--remote', 'local'], check=True, env=env)
print('SEED COMPLETE', flush=True)
"""
    cells = [_code_cell("%%configure -f\n" + json.dumps({"vCores": int(cores)})),
             _code_cell(work)]
    return _notebook(cells)


def launch(sf: int, prefix: str, warehouse: str, cores: int = 0, ref: str = "") -> None:
    """Deploy + run the generation notebook on Fabric and wait for it. Raises on failure."""
    import duckrun

    m = re.match(r"abfss://([^@]+)@[^/]+/[^/]+/Tables$", warehouse.rstrip("/"))
    if not m:
        sys.exit(f"ERROR: remote seed needs an abfss://…/Tables warehouse; got {warehouse!r}")
    ws_guid = m.group(1)
    ref = ref or os.environ.get("GITHUB_SHA", "") or "main"
    cores = cores or auto_cores(sf)
    name = f"tpcdi_seed_sf{sf}"

    nb = build_seed_notebook(sf, prefix, warehouse, ref,
                             cores=cores,
                             jvm_xmx=os.environ.get("TPCDI_JVM_XMX", "4g"),
                             gen_timeout=os.environ.get("TPCDI_GEN_TIMEOUT", "10800"))
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, f"{name}.ipynb")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(nb, fh)
        ws = duckrun.workspace(ws_guid)
        print(f">> deploying {name} to workspace {ws_guid} "
              f"(vCores={cores}, repo@{ref})", flush=True)
        ws.deploy(path, overwrite=True)
    print(f">> running {name} on Fabric compute (kept after the run — see the Fabric UI "
          "for live output) ...", flush=True)
    status = ws.run(name)
    print(f">> remote seed generation finished: {status}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, required=True)
    ap.add_argument("--prefix", default=os.environ.get("TPCDI_ONELAKE_PREFIX", "tpcdi"))
    ap.add_argument("--warehouse", default=os.environ.get("WAREHOUSE_PATH", ""))
    ap.add_argument("--cores", type=int, default=int(os.environ.get("TPCDI_REMOTE_CORES", "0")),
                    help="Fabric notebook vCores (0 = scale with --sf: 8@sf100 doubling per "
                         "10x, cap 64)")
    ap.add_argument("--ref", default="", help="git ref of djouallah/tpcdi to run remotely "
                                              "(default: GITHUB_SHA env, else main)")
    args = ap.parse_args()
    if not args.warehouse:
        sys.exit("ERROR: --warehouse (or WAREHOUSE_PATH) required")
    launch(args.sf, args.prefix, args.warehouse, cores=args.cores, ref=args.ref)


if __name__ == "__main__":
    main()
