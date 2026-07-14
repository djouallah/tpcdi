# TPC-DI on duckrun

This is a port of **TPC-DI** — the TPC's data-integration (ETL) benchmark — to duckrun,
and it stands entirely on the shoulders of
[shannon-barrow/databricks-tpc-di](https://github.com/shannon-barrow/databricks-tpc-di).
Huge thanks to **Shannon Barrow**: the SQL transforms here are ported from that project (its
`Snowflake_CSV` dialect — the closest to DuckDB), and its vendored, standalone **DIGen** data
generator is what we fetch and drive at build time (see below). Go star the upstream repo.

Multi-format source files (pipe/CSV text, fixed-width FINWIRE, and CustomerMgmt
XML) are transformed by dbt models running in DuckDB and materialized as Delta
Lake tables, locally or on OneLake. It exercises exactly duckrun's strengths:
SCD Type 2 dimensions, dimensional joins over effective-date windows, and Delta
output that runs identically local ↔ Fabric.

![The built TPC-DI warehouse in the Fabric OneLake explorer — the `tpcdi` lakehouse with a per-scale-factor schema (`tpcdi100`) holding every dimension and fact as a Delta table.](onelake.png)

## Two modes, two self-contained projects

The repo holds **two independent dbt projects**, each a complete, self-contained way to run
the benchmark. They share only the raw OneLake seed and write **different schemas**, so they
never collide and never wait on each other:

| Folder | Mode | How it loads | Validation |
|---|---|---|---|
| [`singlepass/`](singlepass/) | **Single-pass** | one `dbt run` folds Batch1 historical + Batch2/3 CDC into the final SCD2 warehouse | `dbt test` (per-model uniqueness / FK / SCD2 grain / enum domains) |
| [`sequential/`](sequential/) | **Sequential (3-batch)** | the spec's real model: historical load (Batch1) → two incremental CDC batches, with a per-batch `DImessages` checkpoint between each | `dbt test` **plus the COMPLETE Appendix-A audit** (130 checks, no WARN tier) |

Both produce the same star schema; the sequential mode additionally reproduces the spec's
between-batch checkpoints, which is what lets it run the full `automated_audit.sql` (the
DImessages-driven per-batch checks a single-pass load can't produce). Each folder has its own
`dbt_project.yml`, `profiles.yml`, `packages.yml`, `macros/`, `models/`, and `scripts/` —
nothing dbt-level is shared. The single-pass warehouse lands in `Tables/tpcdi<N>`, the
sequential one in `Tables/tpcdi<N>_seq`.

## Setup

Each mode has its own CI workflow — [`tpc_di.yml`](.github/workflows/tpc_di.yml) (single-pass)
and [`tpc_di_sequential.yml`](.github/workflows/tpc_di_sequential.yml) (sequential) — both
running on **Microsoft Fabric OneLake**. To run them in your own fork you need:

- Two **repo secrets** — `AZURE_CLIENT_ID` and `AZURE_TENANT_ID` — for a Fabric service
  principal / app registration with a **federated credential** trusting this repo (OIDC; no
  client secret is stored). The principal needs contributor rights on the target workspace.
- The target **workspace GUID** is set as `WS_ID` in each workflow env (edit it to yours). The
  workflows create a schema-enabled lakehouse named **`tpcdi`** in that workspace if it doesn't
  exist, and write both the seed (`Files/tpcdi/sf<N>`) and the warehouse (`Tables/tpcdi<N>` /
  `Tables/tpcdi<N>_seq`) there.
- Optionally a repo **variable** `TPCDI_SF` to set the default scale factor (else `3`);
  `workflow_dispatch` takes a `scale_factor` input.

No secret ever pins a token — a fresh OneLake storage token is minted from the OIDC login as
needed (including mid-build, so multi-hour large-SF runs survive the ~1h token lifetime).

The two workflows are **fully independent**: each creates/resolves the lakehouse and streams
its own seed idempotently (a fast cache-hit when the other already produced it), and each fires
only on changes to its own folder — so a single-pass edit never reruns the sequential load and
vice-versa. `requirements.txt` (the shared Python env) triggers both.

## What it builds

Both modes produce the same dimensional warehouse:

- **Dimensions:** `DimDate`, `DimTime`, `DimBroker`, `DimCompany`*, `DimSecurity`*,
  `DimCustomer`*, `DimAccount`*, `DimTrade`  (`*` = SCD Type 2)
- **Facts:** `FactCashBalances`, `FactHoldings`, `FactWatches`, `FactMarketHistory`
- **Reference / other:** `Industry`, `StatusType`, `TaxRate`, `TradeType`,
  `Financial`, `Prospect`, plus staging (`FinWire`, `stg_customermgmt`, `BatchDate`)

The sequential mode also maintains a `DImessages` validation log and an `Audit` answer-key
table (from the PDGF `*_audit.csv` files) that its full audit reads.

## Layout

```
singlepass/     single-pass dbt project (one dbt run; dbt test)
sequential/     sequential 3-batch dbt project (batches + DImessages + full Appendix-A audit)
.github/        two independent ETL workflows (tpc_di.yml, tpc_di_sequential.yml) + queries/docs
requirements.txt  the one shared Python env (duckrun + obstore) for both workflows
README · LICENSE · onelake.png
```

Inside each project folder (both the same shape):

```
dbt_project.yml / profiles.yml / packages.yml
macros/tpcdi.sql    shared read_pipe / read_csvfile / read_fixed helpers + xml/sk/status macros
models/             the dbt models (see each folder)
scripts/            generate_data.py + stream_seed.py + seed_manifest.py (per-table generate→
                    mint→copy→delete to OneLake), and the mode's runner (run_benchmark.py /
                    run.py)
```

Sequential adds `sequential/sql/` (the between-batch `batch_initial` / `batch_complete` /
`batch_validation` / `visibility_1` / `visibility_2` / `audit_alerts` inserts, plus the ported
`automated_audit.sql`) and `sequential/tools/run_sequential_audit.py`. Single-pass keeps
`singlepass/queries/` + `singlepass/scripts/run_queries.py` (see below) and
`singlepass/tools/check_catalog_stats.py`.

## Querying the warehouse

TPC-DI is an ETL benchmark — it defines no query workload. `singlepass/queries/` adds a small
set of plain analytical `SELECT`s over the finished star schema (portfolio value by tier,
commission by broker/quarter, market-close trends, watch-list activity, financials by industry,
…), so you can exercise the *data* and not just the load.

`singlepass/scripts/run_queries.py` runs every `singlepass/queries/*.sql` through
`duckrun.connect(..., read_only=True)` and prints row counts, timings and a small preview.
Because the connection is **read-only** it cannot touch the warehouse, so it is completely
independent of the ETL phase.

```bash
# local, against a warehouse built by run_benchmark.py
WAREHOUSE_PATH=./warehouse python singlepass/scripts/run_queries.py

# OneLake
WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \
    ONELAKE_TOKEN=... python singlepass/scripts/run_queries.py
```

A **separate** CI workflow (`.github/workflows/queries.yml`) runs these against the live OneLake
`tpcdi` warehouse on demand (`workflow_dispatch`) or when `singlepass/queries/**` changes. It has
its own concurrency group (not the ETL group), does no generation / `dbt run` / write, and only
reads — so it never interferes with the load pipeline.

## Running it

Requires a JDK (for PDGF) and network access (to fetch the datagen toolkit and install the
`webbed` DuckDB community extension).

**Single-pass** — one command does generate → dbt run → test:

```bash
python singlepass/scripts/run_benchmark.py --sf 3           # local ./warehouse
# OneLake:
WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \
ONELAKE_TOKEN=<bearer> \
python singlepass/scripts/run_benchmark.py --sf 3 --target onelake
```

**Sequential** — runs the three batches in order, writes the DImessages checkpoints, then the
full audit (`--batches N` runs only batches 1..N; the audit needs the full 3):

```bash
WAREHOUSE_PATH=./warehouse DBT_SCHEMA=tpcdi3_seq \
python sequential/scripts/run.py --sf 3                     # local
# OneLake:
WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \
TPCDI_DIR=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Files/tpcdi/sf3 \
ONELAKE_TOKEN=<bearer> DBT_SCHEMA=tpcdi3_seq \
python sequential/scripts/run.py --sf 3 --target onelake --batches 3
```

Both CI workflows run their mode **on OneLake**, on every push that touches that mode's folder:

1. Create the dedicated **`tpcdi` lakehouse** if it doesn't exist (Fabric REST, schema-enabled).
2. `stream_seed.py` generates the seed **one PDGF table at a time** and streams each straight to
   OneLake — for each table: `-start <table>` → local `/mnt` → (split `CustomerMgmt.xml` into
   chunks) → **mint a fresh token → obstore multipart upload to `Files/tpcdi/sf<N>` → write a
   per-table manifest → delete local → next**. The generator's `*_audit.csv` answer-key files ride
   along in the same batch dirs. Local disk therefore only ever holds a single table, so any scale
   factor fits the runner; PDGF is seed-deterministic, so the per-table output is byte-identical to
   a full run. **Resumable:** it lists what is already in `Files/tpcdi/sf<N>` and skips every table
   whose files are present at their recorded byte size (per-table manifests under `_manifest/`), so
   a re-run regenerates only the missing tables, and a full cache hit does no generation at all.
3. A fresh OneLake token is minted, then the mode's driver reads the seed **from OneLake Files**
   (globs work over `abfss://`) and writes the Delta tables to **`Tables/tpcdi<sf>`** (single-pass)
   or **`Tables/tpcdi<sf>_seq`** (sequential) — the runner never hosts the data. A large SF can
   outlast the ~1h token, so the adapter re-mints mid-run via GitHub OIDC.
4. `dbt test` validates the built warehouse over `abfss://` — per-model uniqueness, referential
   integrity, SCD2 grain, and enum domains.
5. **Sequential only:** `tools/run_sequential_audit.py` runs the full Appendix-A audit against the
   finished warehouse + DImessages (see [Sequential audit](#sequential-audit)). **Blocking:** any
   non-OK check fails the job.

The scale factor is `${{ inputs.scale_factor }}` (workflow_dispatch) → repo variable `TPCDI_SF`
→ `3` (the default; also the TPC-DI spec minimum). Changing it uses a fresh `sf<N>` seed cache,
so it regenerates once for that SF. Large factors need headroom, so the seed is staged on `/mnt`,
the generation watchdog is 3h (`TPCDI_GEN_TIMEOUT`), and the PDGF heap is `TPCDI_JVM_XMX` (4g in CI).

## Notes & design choices

- **Data generator — we drive PDGF, not DIGen.** DIGen is TPC-licensed and not
  vendored; `generate_data.py` shallow-clones the dbx repo (which carries the whole
  standalone DIGen + PDGF toolkit). It does **not** call `DIGen.jar`, though: DIGen
  shells out to `java -jar pdgf.jar …`, but this datagen's `pdgf.jar` manifest omits
  `plugins/tpc-di.jar`, and PDGF discovers its generators/timeframe-modes by scanning
  `java.class.path` — which under `-jar` holds only `pdgf.jar`. So the plugin is never
  seen and the parse dies on `tpc.di.generators.HRJobIdGenerator was not found` (then
  the custom `gen_ReferenceGenerator from="DailyMarket-…"` mode). We instead run
  `java -cp pdgf.jar:plugins/tpc-di.jar:extlib/* pdgf.Controller -sf N000 -start
  -closeWhenDone` — the plugin on `-cp` puts it on `java.class.path` so the scan
  registers everything. We pass **no** `-o` (PDGF splices it into a javassist-compiled
  file template un-quoted, which won't compile) and let PDGF write its default
  `output/Batch{1,2,3}`, which the script moves under the staging dir; and we keep
  stdin open after the license ENTER+YES so PDGF's shell thread can't flood-kill the
  run. `-sf` is the TPC-DI factor ×1000 (what DIGen applies). No Spark. Override the
  source with `DBX_TPCDI_REPO` / `DBX_TPCDI_REF`.

  Getting this Java toolchain to run headless was a **freaking nightmare** — the
  jar-in-jar loader that breaks on Java 9+, the `java.class.path` plugin scan, the
  un-quoted `-o` file template that won't compile, the stdin EOF that flood-kills the
  run mid-generation — every one of them a separate rabbit hole. I would genuinely love
  for someone to port the TPC-DI generator to **Rust** and free us all from the JVM.
- **Seed is generated once, then cached in OneLake — resumably, per table.** `stream_seed.py`
  lists `Files/tpcdi/sf<N>` and, for each of the 16 PDGF tables, skips generation when every file
  that table produced is already present at its recorded byte size — tracked by a small per-table
  manifest at `Files/tpcdi/sf<N>/_manifest/<table>.json` (`seed_manifest.py`). "Present" means
  present-and-same-size (obstore listing), which catches truncated/half-uploaded files — PDGF is
  deterministic, so an identical size is a strong correctness signal. Both modes stream to the same
  `Files/tpcdi/sf<N>` prefix, so whichever runs first pays the generation cost and the other gets a
  fast cache hit.
- **XML — and why we chunk it.** DuckDB has no first-party XML reader, so
  `stg_customermgmt` uses the [`webbed`](https://github.com/teaguesterling/duckdb_webbed)
  community extension (`INSTALL webbed FROM community; LOAD webbed`, via the model's
  `pre_hook`), materialized as a table so only that one model needs the extension.
  `CustomerMgmt.xml` scales with SF (~9MB at sf3, ~300MB at sf100) and webbed **silently
  returns zero rows** on a multi-hundred-MB document (no error — it just parses nothing,
  which is how an sf100 run first looked "green" while the customer/account/trade/fact
  tables came out near-empty). So `generate_data.py` splits it into `CustomerMgmt_NNNN.xml`
  chunks (~20k `<Action>` elements each), which the model reads with the
  `CustomerMgmt_*.xml` glob (per-file parse, bounded memory).
- **Headerless typed files.** duckrun keeps source scans to auto-detect, so the
  models read the raw files directly with an explicit `read_csv(columns=…)`
  (`macros/tpcdi.sql`) rather than declaring dbt sources.
- **SCD2.** Company/Security/Customer/Account versioning is done with the reference
  project's window-function SQL (effective/end dating driven by batch dates and CDC
  actions), not dbt snapshots — dbt's hash-based snapshot semantics don't match
  TPC-DI's dating rules. The sequential mode expresses the incremental SCD2 update as a
  single duckrun `merge` (new + close-current + prospect-update rows unioned, disjoint by
  surrogate key), since a native two-`WHEN MATCHED` MERGE isn't expressible in one dbt merge.
- **Single-*pass* vs single-*batch*.** The single-pass mode reads `Batch[123]/…` (all three
  batches: Batch1 historical + Batch2/3 CDC inserts/updates), applies the `cdc_flag`, and folds
  everything into the final SCD2 dimensions and facts in one `dbt run` — the finished warehouse is
  correct, but there are no between-batch checkpoints. The **sequential** mode runs the three
  batches as three separate, sequentially-validated executions (`is_incremental()` branching:
  batch 1 = the historical `--full-refresh` load, batches 2–3 = CDC merges), writing the
  `DImessages` validation log between each — which is what the full Appendix-A audit needs.
- **dbt-native tests — the per-model transformation gate.** A `dbt test` step (both modes, after
  the build) runs standard generic tests defined in `models/**/_*.yml` over the built OneLake Delta
  tables: surrogate-key uniqueness + `not_null`, foreign-key `relationships` to the parent
  dimension, `dbt_utils.unique_combination_of_columns` for the SCD2 grain (business key +
  `effectivedate`), and `accepted_values` enum domains (Status, Gender, Issue, ExchangeID) — a
  NULL/duplicate SK or an orphan FK fails the exact model. Read-only over `delta_scan` views.

### Sequential audit

`sequential/tools/run_sequential_audit.py` runs the **complete** TPC-DI Appendix-A *automated
audit* — all 130 checks, ported verbatim to DuckDB in `sequential/sql/automated_audit.sql` from
`shannon-barrow/databricks-tpc-di`
`src/incremental_batches/audit_validation/automated_audit.sql`. There is **no reduced subset and
no WARN tier**: every check is FAIL-fatal, and the run exits nonzero if any check's `Result` is
not `OK`. It runs read-only (same duckrun connection style as the query runner) as the last step
of a full 3-batch run.

The audit reads three things, all already materialized as Delta tables in the schema:

- the finished **warehouse** dimension/fact tables;
- the **`Audit`** answer-key table — the PDGF `*_audit.csv` files (which ride along in the seed
  under `Batch{1,2,3}/`), loaded by the orchestrator;
- the **`DImessages`** log the orchestrator writes between batches, which the incremental checks
  read: `batch_initial.sql` (the batch-0 empty-DW checkpoint), `batch_complete.sql` (the
  per-batch Phase Complete Record), `batch_validation.sql` (24 row-count / referential-integrity
  Validation rows per batch), `visibility_1.sql` / `visibility_2.sql` (the two Data Visibility
  snapshots), and `audit_alerts.sql` (the tier / DOB / commission / fee / SPRating Alert rows).

This covers everything the single-pass load *couldn't* — the DImessages validation-report and
phase-complete counts, the per-batch row-count reconciliations against the answer keys, the
alert-count checks, and the Batch / Data-visibility row-count regressions — plus all the
end-state structural invariants (SCD2 date sanity, referential integrity, attribute domains, and
the FactMarketHistory `52-week-low ≤ day-low ≤ close ≤ day-high ≤ 52-week-high` check). Each
check prints `PASS/FAIL | test | batch | detail`.

## License

[MIT](LICENSE). The ported SQL transforms and the DIGen/PDGF data generator originate from
[shannon-barrow/databricks-tpc-di](https://github.com/shannon-barrow/databricks-tpc-di) and
remain under their respective upstream licenses; DIGen itself is TPC-licensed and is fetched
at build time, never vendored here.
