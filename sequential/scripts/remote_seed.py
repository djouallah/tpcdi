"""Run heavy TPC-DI phases ON Fabric compute — seed generation and the Appendix-A audit.

The GitHub runner is 4 vCPUs, ~16 GiB RAM, ~14 GiB free disk. Two phases outgrow it:

* **Seed generation** (sf > 370): the per-table streamer only ever keeps one table on local
  disk, so the ceiling is the LARGEST SINGLE TABLE — at sf1000 (~1 TB warehouse) a single
  table no longer fits.
* **The Appendix-A audit** (sf >= 100): its heaviest check row-multiplies DimTrade × DimDate
  × DimBroker and already spilled ~79 GiB at sf100, past the runner's disk. The audit SQL is
  canonical and untouchable — the fix is bigger hardware, not a rewrite.

Both run instead in a Fabric PYTHON notebook: ~135 GiB local work disk, memory scaling with
vCores, data-local to OneLake, tokens self-acquired via notebookutils. The launcher builds a
notebook that downloads THIS repo at the launching commit (``GITHUB_SHA``) and runs the SAME
script it would have run locally (``--remote local``) as a subprocess — so the freshly
pip-installed duckdb/deltalake load in a clean interpreter (no restartPython dance) and the
logic is identical wherever it executes. Seed generation additionally installs a portable
JDK 8 into the work dir (PDGF's Eclipse jar-in-jar loader breaks on Java 9+; Adoptium
tarball, no root).

duckrun's capture pattern: the whole work cell is wrapped, every step's output is tee'd
(live notebook snapshot + buffer), and a result JSON ``{ok, log}`` is written to OneLake
Files LAST, cleanly, whatever happened — the launcher reads it back (even off a failed job),
prints the remote log, and decides success from the JSON, not the Fabric job state.
Notebooks (``tpcdi_seed_sf<N>`` / ``tpcdi_audit_sf<N>``) are DELETED after any run that
produced a result JSON — the log has already been read back, so nothing lingers in the
workspace. Only a session that died before the task ran keeps its notebook (its snapshot is
the sole forensic evidence); the next launch overwrites it.

Used by ``stream_seed.py --remote auto`` (seed, remote for sf > 370) and
``tools/run_sequential_audit.py --remote auto`` (audit, remote for sf >= 100); direct use:

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
# runs the SAME project's scripts out of the repo download.
PROJECT = os.path.basename(os.path.dirname(HERE))
REPO_TARBALL = "https://codeload.github.com/djouallah/tpcdi/tar.gz/{ref}"
JDK8_URL = ("https://api.adoptium.net/v3/binary/latest/8/ga/linux/x64/jdk/hotspot/"
            "normal/eclipse")


def auto_cores(sf: int) -> int:
    """Same sizing as run.py's remote dbt — doubling per HALF decade: 8 vCores at sf100,
    16 from ~sf320, 32 at sf1000, 64 from ~sf3200 (Fabric's cap; valid sizes only)."""
    return min(64, 8 * 2 ** int(2 * math.log10(max(sf, 100) / 100)))


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


def build_task_notebook(cfg: dict, cores: int) -> dict:
    """A notebook that runs one repo script remotely. ``cfg`` keys: ``warehouse``, ``ref``,
    ``repo_url``, ``script_glob`` (repo-relative glob of the script), ``script_args``,
    ``env`` (extra vars for the subprocess), ``need_jdk``, ``workdir``, ``result``
    (OneLake object path of the result JSON), ``label`` (log prefix)."""
    cfg = dict(cfg, jdk_url=JDK8_URL)
    work = f"""\
import glob, io, json, os, re, shutil, subprocess, sys, tarfile, traceback, urllib.request
CFG = {json.dumps(cfg)}
LOG = io.StringIO()

def say(msg):
    print(msg, flush=True)
    LOG.write(msg + '\\n')

def untar(src, dst):
    with tarfile.open(src) as t:
        try:
            t.extractall(dst, filter='fully_trusted')  # keep symlinks + exec bits (JDK)
        except TypeError:
            t.extractall(dst)

def fetch(url, dst):
    # urlretrieve sends no User-Agent and Adoptium/GitHub 403 such requests.
    req = urllib.request.Request(url, headers={{'User-Agent': 'tpcdi-remote/1'}})
    with urllib.request.urlopen(req) as r, open(dst, 'wb') as out:
        shutil.copyfileobj(r, out)

def run_tee(cmd, env=None):
    say('$ ' + ' '.join(cmd))
    p = subprocess.Popen(cmd, env=env, text=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    for line in p.stdout:
        say(line.rstrip('\\n'))
    if p.wait() != 0:
        raise RuntimeError('command failed (exit %s): %s' % (p.returncode, cmd))

ok = False
try:
    WORK = '/home/trusted-service-user/work/' + CFG['workdir']
    os.makedirs(WORK, exist_ok=True)
    os.chdir(WORK)
    run_tee([sys.executable, '-m', 'pip', 'install', '-q', 'duckrun', 'obstore'])

    if CFG['need_jdk']:
        if not glob.glob('jdk/*/bin/java'):
            say('downloading portable JDK 8 (Adoptium) ...')
            fetch(CFG['jdk_url'], 'jdk8.tgz')
            untar('jdk8.tgz', 'jdk')
            os.remove('jdk8.tgz')
        os.environ['PATH'] = (os.path.abspath(glob.glob('jdk/*/bin')[0]) + os.pathsep
                              + os.environ['PATH'])
        run_tee(['java', '-version'])

    say('fetching djouallah/tpcdi @ ' + CFG['ref'])
    fetch(CFG['repo_url'], 'repo.tgz')
    shutil.rmtree('repo', ignore_errors=True)
    untar('repo.tgz', 'repo')
    os.remove('repo.tgz')
    script = glob.glob('repo/*/' + CFG['script_glob'])[0]

    import notebookutils
    env = dict(os.environ)
    env.update(CFG['env'])
    env['ONELAKE_TOKEN'] = notebookutils.credentials.getToken('storage')
    say('%s: running %s' % (CFG['label'], CFG['script_glob']))
    run_tee([sys.executable, script] + CFG['script_args'], env=env)
    ok = True
    say(CFG['label'].upper() + ' COMPLETE')
except BaseException:
    LOG.write(traceback.format_exc())
    print(traceback.format_exc(), flush=True)

# Result JSON to OneLake Files — written LAST, cleanly, whatever happened above.
# (DFS DELETE first: OneLake 409s a PUT onto an existing blob, and obstore.delete uses a
# bulk-batch API OneLake rejects — same dance as seed_manifest.delete_object.)
from obstore.store import AzureStore
import notebookutils, obstore
tok = notebookutils.credentials.getToken('storage')
ws_name, host, lh = re.match(r'abfss://([^@]+)@([^/]+)/([^/]+)/Tables$',
                             CFG['warehouse'].rstrip('/')).groups()
req = urllib.request.Request('https://%s/%s/%s/%s' % (host, ws_name, lh, CFG['result']),
                             method='DELETE', headers={{'Authorization': 'Bearer ' + tok}})
try:
    urllib.request.urlopen(req)
except Exception:
    pass
store = AzureStore.from_url('abfss://%s@%s/%s/' % (ws_name, host, lh), bearer_token=tok)
body = json.dumps({{'ok': ok, 'log': LOG.getvalue()[-200000:]}}).encode()
obstore.put(store, CFG['result'], body)
print('result written: ' + CFG['result'], flush=True)
"""
    cells = [_code_cell("%%configure -f\n" + json.dumps({"vCores": int(cores)})),
             _code_cell(work)]
    return _notebook(cells)


def launch_task(name: str, cfg: dict, cores: int) -> None:
    """Deploy the task notebook (overwrite), run it on Fabric, read the result JSON back,
    print the remote log, and exit non-zero if the task failed."""
    import duckrun

    warehouse = cfg["warehouse"]
    m = re.match(r"abfss://([^@]+)@[^/]+/[^/]+/Tables$", warehouse.rstrip("/"))
    if not m:
        sys.exit(f"ERROR: remote execution needs an abfss://…/Tables warehouse; got {warehouse!r}")
    ws_guid = m.group(1)

    nb = build_task_notebook(cfg, cores)
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, f"{name}.ipynb")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(nb, fh)
        ws = duckrun.workspace(ws_guid)
        print(f">> deploying {name} to workspace {ws_guid} "
              f"(vCores={cores}, repo@{cfg['ref']})", flush=True)
        item_id = ws.deploy(path, overwrite=True)
    # Clear any prior run's result FIRST, so whatever we read back afterwards is this run's
    # verdict, never a stale ok:true from an earlier launch of the same task.
    _delete_result(warehouse, cfg["result"])

    # A job that fails WITHOUT writing the result JSON never ran our code — a session-level
    # failure (typically transient capacity contention: another Fabric job holding the
    # vCores). Those get one delayed retry. A job that wrote {ok: false} really ran and
    # really failed — never retried.
    import time
    for attempt in (1, 2):
        print(f">> running {name} on Fabric compute (attempt {attempt}/2; kept after the "
              "run — see the Fabric UI for live output) ...", flush=True)
        job_err = None
        try:
            status = ws.run(name)
            print(f">> remote job state: {status}", flush=True)
        except Exception as exc:  # noqa: BLE001 — read the result log back even on a failed job
            job_err = exc

        res = _read_result(warehouse, cfg["result"])
        if res is not None:
            print(">> ---- remote log " + "-" * 60, flush=True)
            print(res.get("log", "").rstrip(), flush=True)
            print(">> ---- end remote log " + "-" * 56, flush=True)
            # The full log is preserved above — the notebook has served its purpose.
            _delete_notebook(ws_guid, item_id, name)
            if res.get("ok"):
                print(f">> remote {cfg['label']} SUCCEEDED", flush=True)
                return
            sys.exit(f">> remote {cfg['label']} FAILED — see the remote log above")
        if attempt == 1:
            print(f">> job died before the task ran ({job_err}) — likely transient capacity "
                  "contention; retrying in 120s", flush=True)
            time.sleep(120)
    # Session died before our code ran on both attempts — KEEP the notebook: its snapshot is
    # the only forensic evidence there is.
    print(f">> notebook {name} kept in the workspace for diagnosis", flush=True)
    raise job_err


def _delete_notebook(ws_guid: str, item_id: str, name: str) -> None:
    """Best-effort DELETE of the run notebook — the workspace stays clean; the run's log has
    already been printed from the result JSON."""
    import urllib.request

    from duckrun import auth
    req = urllib.request.Request(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws_guid}/items/{item_id}",
        method="DELETE", headers={"Authorization": f"Bearer {auth.get_fabric_token()}"})
    try:
        urllib.request.urlopen(req)
        print(f">> notebook {name} deleted from the workspace", flush=True)
    except Exception as exc:  # noqa: BLE001 — a leftover notebook is cosmetic, never fatal
        print(f">> (could not delete notebook {name}: {exc})", flush=True)


def _delete_result(warehouse: str, result_obj: str) -> None:
    """DFS-delete the result object (404 = fine) — obstore.delete is OneLake-broken."""
    import urllib.error
    import urllib.request

    from duckrun import auth
    ws_name, host, lh = re.match(r"abfss://([^@]+)@([^/]+)/([^/]+)/Tables$",
                                 warehouse.rstrip("/")).groups()
    req = urllib.request.Request(f"https://{host}/{ws_name}/{lh}/{result_obj}",
                                 method="DELETE",
                                 headers={"Authorization": f"Bearer {auth.get_onelake_token()}"})
    try:
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise


def _read_result(warehouse: str, result_obj: str):
    """The notebook's result JSON from OneLake Files, or None if it never got written."""
    import obstore
    from obstore.store import AzureStore

    from duckrun import auth
    ws_name, host, lh = re.match(r"abfss://([^@]+)@([^/]+)/([^/]+)/Tables$",
                                 warehouse.rstrip("/")).groups()
    store = AzureStore.from_url(f"abfss://{ws_name}@{host}/{lh}/",
                                bearer_token=auth.get_onelake_token())
    try:
        return json.loads(bytes(obstore.get(store, result_obj).bytes()))
    except Exception:  # noqa: BLE001 — absent = notebook died before writing it
        return None


def launch(sf: int, prefix: str, warehouse: str, cores: int = 0, ref: str = "") -> None:
    """Remote SEED GENERATION: the project's stream_seed.py on Fabric compute (with JDK 8)."""
    cfg = {
        "label": "seed generation",
        "warehouse": warehouse,
        "ref": ref or os.environ.get("GITHUB_SHA", "") or "main",
        "need_jdk": True,
        "workdir": "tpcdi_seed",
        "script_glob": f"{PROJECT}/scripts/stream_seed.py",
        "script_args": ["--sf", str(sf), "--prefix", prefix,
                        "--warehouse", warehouse, "--remote", "local"],
        "env": {"WAREHOUSE_PATH": warehouse,
                "TPCDI_STAGING": "/home/trusted-service-user/work/tpcdi_seed/staging",
                "TPCDI_WORK": "/home/trusted-service-user/work/tpcdi_seed/work",
                "TPCDI_JVM_XMX": os.environ.get("TPCDI_JVM_XMX", "4g"),
                "TPCDI_GEN_TIMEOUT": os.environ.get("TPCDI_GEN_TIMEOUT", "10800")},
        "result": f"Files/{prefix.strip('/')}/_remote_seed_result.json",
        "repo_url": None,  # filled below
    }
    cfg["repo_url"] = REPO_TARBALL.format(ref=cfg["ref"])
    launch_task(f"tpcdi_seed_sf{sf}", cfg, cores or auto_cores(sf))


def launch_audit(sf: int, schema: str, warehouse: str, cores: int = 0, ref: str = "") -> None:
    """Remote AUDIT: the project's tools/run_sequential_audit.py on Fabric compute — the
    canonical Appendix-A SQL, unchanged, just running where the memory/disk is (its heaviest
    check spilled ~79 GiB at sf100, past the runner)."""
    cfg = {
        "label": "audit",
        "warehouse": warehouse,
        "ref": ref or os.environ.get("GITHUB_SHA", "") or "main",
        "need_jdk": False,
        "workdir": "tpcdi_audit",
        "script_glob": f"{PROJECT}/tools/run_sequential_audit.py",
        "script_args": ["--warehouse", warehouse, "--schema", schema, "--remote", "local"],
        "env": {"WAREHOUSE_PATH": warehouse, "DBT_SCHEMA": schema, "TPCDI_SF": str(sf)},
        "result": f"Files/tpcdi/_remote/audit_{schema}.json",
        "repo_url": None,
    }
    cfg["repo_url"] = REPO_TARBALL.format(ref=cfg["ref"])
    launch_task(f"tpcdi_audit_sf{sf}", cfg, cores or auto_cores(sf))


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
