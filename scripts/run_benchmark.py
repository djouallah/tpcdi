"""Drive the full TPC-DI load on duckrun: generate -> dbt run -> dbt test.

Two modes:
  --mode singlepass (default): one `dbt run` reads Batch1 (historical) plus Batch2/3
    (incremental CDC) together and produces the complete SCD2 warehouse end-state.
  --mode sequential: delegates to scripts/run_sequential.py, which runs the spec's real
    execution model — historical load (Batch1) then two incremental batches with
    per-batch DImessages validation — and passes the complete Appendix-A audit.

Local:
    python run_benchmark.py --sf 3
    python run_benchmark.py --sf 3 --mode sequential
OneLake (Delta output to a Fabric Lakehouse):
    WAREHOUSE_PATH=abfss://.../Tables ONELAKE_TOKEN=... python run_benchmark.py --sf 3 --target onelake
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(HERE)
# The single-pass dbt project lives in singlepass/ (sequential/ is its sibling).
SP_PROJ = os.path.join(PROJ, "singlepass")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=int, default=int(os.environ.get("TPCDI_SF", "3")))
    ap.add_argument("--mode", choices=["singlepass", "sequential"], default="singlepass")
    ap.add_argument("--target", choices=["local", "onelake"], default="local")
    ap.add_argument("--staging", default=os.environ.get("TPCDI_DIR",
                    os.path.join(PROJ, "staging")))
    ap.add_argument("--skip-generate", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    # Sequential mode is a distinct execution model (3 batches + per-batch validation);
    # run_sequential.py owns it. Forward the same flags.
    if args.mode == "sequential":
        seq = [sys.executable, os.path.join(HERE, "run_sequential.py"),
               "--sf", str(args.sf), "--target", args.target, "--staging", args.staging]
        if args.skip_generate:
            seq.append("--skip-generate")
        if args.force:
            seq.append("--force")
        sys.exit(subprocess.run(seq, env=dict(os.environ)).returncode)

    staging = os.path.abspath(args.staging)
    env = dict(os.environ)
    env["TPCDI_DIR"] = staging
    env["DBT_SCHEMA"] = env.get("DBT_SCHEMA", "tpcdi")
    if args.target == "local":
        env["WAREHOUSE_PATH"] = os.path.abspath(
            env.get("WAREHOUSE_PATH", os.path.join(PROJ, "warehouse")))
    elif not env.get("WAREHOUSE_PATH"):
        sys.exit("ERROR: --target onelake needs WAREHOUSE_PATH (abfss://.../Tables)")

    if not args.skip_generate:
        gen = [sys.executable, os.path.join(HERE, "generate_data.py"),
               "--sf", str(args.sf), "--out", staging]
        if args.force:
            gen.append("--force")
        print(">> generating data", flush=True)
        subprocess.run(gen, check=True, env=env)

    print(">> dbt run", flush=True)
    subprocess.run(
        ["dbt", "run", "--project-dir", SP_PROJ, "--profiles-dir", SP_PROJ],
        check=True, env=env,
    )

    print(">> dbt test", flush=True)
    subprocess.run(
        ["dbt", "test", "--project-dir", SP_PROJ, "--profiles-dir", SP_PROJ],
        check=True, env=env,
    )
    print(">> done.", flush=True)


if __name__ == "__main__":
    main()
