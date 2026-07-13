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

## Setup

The CI (`.github/workflows/tpc_di.yml`) runs the whole benchmark on **Microsoft Fabric
OneLake**. To run it in your own fork you need:

- Two **repo secrets** — `AZURE_CLIENT_ID` and `AZURE_TENANT_ID` — for a Fabric service
  principal / app registration with a **federated credential** trusting this repo (OIDC; no
  client secret is stored). The principal needs contributor rights on the target workspace.
- The target **workspace GUID** is set as `WS_ID` in the workflow env (edit it to yours). The
  workflow creates a schema-enabled lakehouse named **`tpcdi`** in that workspace if it doesn't
  exist, and writes both the seed (`Files/tpcdi/sf<N>`) and the warehouse (`Tables/tpcdi<N>`, one
  schema per scale factor) there.
- Optionally a repo **variable** `TPCDI_SF` to set the default scale factor (else `20`);
  `workflow_dispatch` takes a `scale_factor` input.

No secret ever pins a token — a fresh OneLake storage token is minted from the OIDC login as
needed (including mid-build, so multi-hour large-SF runs survive the ~1h token lifetime).

## What it builds

A single `dbt run` performs the whole load in one pass (Batch1 historical +
Batch2/3 incremental CDC together), producing the dimensional warehouse:

- **Dimensions:** `DimDate`, `DimTime`, `DimBroker`, `DimCompany`*, `DimSecurity`*,
  `DimCustomer`*, `DimAccount`*, `DimTrade`  (`*` = SCD Type 2)
- **Facts:** `FactCashBalances`, `FactHoldings`, `FactWatches`, `FactMarketHistory`
- **Reference / other:** `Industry`, `StatusType`, `TaxRate`, `TradeType`,
  `Financial`, `Prospect`, plus staging (`FinWire`, `ProspectIncremental`,
  `stg_customermgmt`, `BatchDate`)

## Layout

```
models/base/        typed reads of the reference/date files + FinWire split + Prospect
models/silver/      FINWIRE-derived SCD2 dims (DimCompany, DimSecurity) + Financial + DimBroker
models/staging/     stg_customermgmt — CustomerMgmt.xml flattened via the `webbed` extension
models/incremental/ SCD2 DimCustomer/DimAccount/DimTrade, Prospect, and the four facts
macros/tpcdi.sql    shared read_pipe / read_csvfile / read_fixed helpers + xml/sk/status macros
scripts/            stream_seed.py (per-table generate→mint→copy→delete to OneLake — the CI path),
                    generate_data.py (drives PDGF + splits CustomerMgmt.xml), upload_to_onelake.py,
                    run_benchmark.py (local driver)
models/**/_*.yml    dbt-native tests: PK uniqueness, FK relationships, SCD2 grain, enum domains (dbt test)
packages.yml        dbt_utils (unique_combination_of_columns for the SCD2 grain checks)
queries/            plain analytical SELECTs run against the built warehouse (see below)
scripts/run_queries.py  read-only duckrun runner for queries/ (local or OneLake)
tools/              check_catalog_stats.py — asserts the docs catalog carries real Delta stats
```

## Querying the warehouse

TPC-DI is an ETL benchmark — it defines no query workload. `queries/` adds a small set of
plain analytical `SELECT`s over the finished star schema (portfolio value by tier, commission
by broker/quarter, market-close trends, watch-list activity, financials by industry, …), so
you can exercise the *data* and not just the load.

`scripts/run_queries.py` runs every `queries/*.sql` through `duckrun.connect(..., read_only=True)`
and prints row counts, timings and a small preview. Because the connection is **read-only** it
cannot touch the warehouse, so it is completely independent of the ETL phase.

```bash
# local, against a warehouse built by run_benchmark.py
WAREHOUSE_PATH=./warehouse python scripts/run_queries.py

# OneLake
WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \
    ONELAKE_TOKEN=... python scripts/run_queries.py
```

A **separate** CI workflow (`.github/workflows/queries.yml`) runs these against the live OneLake
`tpcdi` warehouse on demand (`workflow_dispatch`) or when `queries/**` changes. It has its own
concurrency group (not the ETL group), does no generation / `dbt run` / write, and only reads —
so it never interferes with the load pipeline.

## Running it

Requires a JDK (for PDGF) and network access (to fetch the datagen toolkit and
install the `webbed` DuckDB community extension). One command does generate →
dbt run → test:

```bash
python scripts/run_benchmark.py --sf 3           # local ./warehouse
```

OneLake (Delta output to a Fabric Lakehouse):

```bash
WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \
ONELAKE_TOKEN=<bearer> \
python scripts/run_benchmark.py --sf 3 --target onelake
```

CI (`.github/workflows/tpc_di.yml`) runs the whole thing **on OneLake**, on every push:

1. Create the dedicated **`tpcdi` lakehouse** if it doesn't exist (Fabric REST, schema-enabled).
2. `stream_seed.py` generates the seed **one PDGF table at a time** and streams each straight to
   OneLake — for each table: `-start <table>` → local `/mnt` → (split `CustomerMgmt.xml` into
   chunks) → **mint a fresh token → obstore multipart upload to `Files/tpcdi/sf<N>` → write a
   per-table manifest → delete local → next**. The generator's `*_audit.csv` answer-key files ride
   along in the same batch dirs (the uploader copies the whole tree). Local disk therefore only ever
   holds a single table, so any scale factor fits the runner; PDGF is seed-deterministic, so the
   per-table output is byte-identical to a full run.
   **Resumable:** it first lists what is already in `Files/tpcdi/sf<N>` and skips every table whose
   files are present at their recorded byte size (per-table manifests under `_manifest/`), so a
   re-run after a partial failure regenerates only the missing tables, and a full cache hit does no
   generation (no datagen clone) at all.
4. A fresh OneLake token is minted, then `dbt run` reads the seed **from OneLake Files**
   (globs work over `abfss://`) and writes the Delta dimension/fact tables to **`Tables/tpcdi<sf>`**
   (one schema per scale factor) — the runner never hosts the data. A large SF can outlast the ~1h
   token, so dbt re-mints and `dbt retry` (resumes from the failed model) on failure, on top of the
   adapter's own mid-run GitHub-OIDC token refresh.
5. `dbt test` validates the built warehouse over `abfss://` — per-model uniqueness, referential
   integrity, SCD2 grain, and enum domains (see below).
6. `tools/run_audit.py` runs the **end-state audit** — the single-pass-valid subset of the TPC-DI
   Appendix-A checks — against the finished warehouse plus the PDGF `*_audit.csv` answer keys
   (see [End-state audit](#end-state-audit)). It is **blocking**: any FAIL fails the job.

The scale factor is `${{ inputs.scale_factor }}` (workflow_dispatch) → repo variable `TPCDI_SF`
→ `20` (the default). It scales to `100` (~10GB seed / ~163M source rows / ~108M warehouse rows)
and beyond (the TPC-DI spec minimum is `3`). Changing it uses a fresh `sf<N>` seed cache, so
it regenerates once for that SF. Large factors need headroom, so the seed is staged on `/mnt`
(the runner's ~65GB disk), the generation watchdog is 3h (`TPCDI_GEN_TIMEOUT`), and the PDGF
heap is `TPCDI_JVM_XMX` (4g in CI).

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
  manifest at `Files/tpcdi/sf<N>/_manifest/<table>.json` (`seed_manifest.py`). A partial run (e.g.
  killed at a large SF) therefore resumes and regenerates only the missing tables rather than
  starting over, and a complete seed is a fast no-op (no datagen clone). "Present" means
  present-and-same-size (obstore listing), which catches truncated/half-uploaded files — PDGF is
  deterministic, so an identical size is a strong correctness signal. Uploads use obstore multipart
  (`stream_seed._upload`), and dbt reads the seed straight from OneLake — the runner only hosts each
  table's bytes transiently, during its generation.
- **XML — and why we chunk it.** DuckDB has no first-party XML reader, so
  `stg_customermgmt` uses the [`webbed`](https://github.com/teaguesterling/duckdb_webbed)
  community extension (`INSTALL webbed FROM community; LOAD webbed`, via the model's
  `pre_hook`), materialized as a table so only that one model needs the extension.
  `CustomerMgmt.xml` scales with SF (~9MB at sf3, ~300MB at sf100) and webbed **silently
  returns zero rows** on a multi-hundred-MB document (no error — it just parses nothing,
  which is how an sf100 run first looked "green" while the customer/account/trade/fact
  tables came out near-empty). So `generate_data.py` splits it into `CustomerMgmt_NNNN.xml`
  chunks (~20k `<Action>` elements each), which the model reads with the
  `CustomerMgmt_*.xml` glob (per-file parse, bounded memory) — without the split the tables came
  out near-empty while the run still looked green.
- **Headerless typed files.** duckrun keeps source scans to auto-detect, so the
  models read the raw files directly with an explicit `read_csv(columns=…)`
  (`macros/tpcdi.sql`) rather than declaring dbt sources.
- **SCD2.** Company/Security/Customer/Account versioning is done with the reference
  project's window-function SQL (effective/end dating driven by batch dates and CDC
  actions), not dbt snapshots — dbt's hash-based snapshot semantics don't match
  TPC-DI's dating rules.
- **CDC is fully processed — the load is single-*pass*, not single-*batch*.** The
  incremental models read `Batch[123]/…` (all three batches: Batch1 historical +
  Batch2/3 CDC inserts/updates), apply the `cdc_flag`, and fold everything into the
  final SCD2 dimensions and facts in one `dbt run`. What we *don't* do is run the three
  batches as three separate, sequentially-audited executions — we compute the final
  end-state directly. The finished warehouse is the same; only the between-batch
  checkpoints are absent.
- **dbt-native tests — the per-model transformation gate.** A `dbt test` step (CI, after the
  build + docs) runs standard generic tests defined in `models/**/_*.yml` over the built OneLake
  Delta tables: surrogate-key uniqueness + `not_null`, foreign-key `relationships` to the parent
  dimension, `dbt_utils.unique_combination_of_columns` for the SCD2 grain (business key +
  `effectivedate`), and `accepted_values` enum domains (Status, Gender, Issue, ExchangeID) — a
  NULL/duplicate SK or an orphan FK fails the exact model. Read-only over `delta_scan` views;
  needs only the OneLake bearer token.

### End-state audit

`tools/run_audit.py` restores the **end-state subset** of the TPC-DI Appendix-A *automated audit*
(ported from `shannon-barrow/databricks-tpc-di`
`src/incremental_batches/audit_validation/automated_audit.sql`). It loads every PDGF `*_audit.csv`
answer-key file (which rides along in the seed under `Batch{1,2,3}/`) into an `Audit` table, then
validates the finished warehouse against it. Read-only, via the same duckrun connection as
`scripts/run_queries.py`; the answer keys go into a DuckDB TEMP table so nothing touches Delta.

Each check prints `PASS/FAIL/WARN | test | batch | expected | actual`. It runs in CI right after
`dbt test` and is **blocking** — any FAIL exits nonzero and fails the job.

- **Kept:** every check that reads only the finished warehouse tables and/or the `Audit` answer
  keys — final/per-batch row counts vs the audit values, attribute-domain checks (gender,
  marketingnameplate, S&P rating, exchange/issue/status enums), SCD2 date sanity (EndDate
  alignment, no overlap, end-of-time, IsCurrent), referential integrity, and the FactMarketHistory
  52-week `52-week-low ≤ day-low ≤ close ≤ day-high ≤ 52-week-high` spot check.
- **`WARN` (kept, non-fatal):** checks whose correctness depends on per-batch *source*
  attribution or the Audit `Batch` FirstDay/LastDay date windows — both ambiguous under a
  single-pass load, where a row's `batchid` is the batch that sourced it, not a load checkpoint.
- **Skipped:** every check that reads the `DImessages` validation log or the Audit meta-rows —
  per-batch validation-report counts, phase-complete (PCR) records, `Audit table batches/sources`
  meta-checks, and the Batch / Data-visibility row-count regressions. A single-pass load produces
  no `DImessages` log and no between-batch checkpoints, so these have nothing to compare against
  (see below). The full list is documented at the top of `tools/run_audit.py`.

## Not yet covered (follow-ups)

- **Per-batch Appendix-A audit + DImessages.** The parts of the spec's *automated audit* that
  read the `DImessages` validation log are inherently per-batch (row counts / phase-complete
  records after each of the 3 batches) and assume the 3-separate-runs execution model. Our
  single-pass load has no between-batch checkpoints or `DImessages` log, so those checks are
  skipped by `tools/run_audit.py` (the end-state subset above covers everything that *can* be
  validated on the finished warehouse); the full per-batch audit is a separate, larger build.

## License

[MIT](LICENSE). The ported SQL transforms and the DIGen/PDGF data generator originate from
[shannon-barrow/databricks-tpc-di](https://github.com/shannon-barrow/databricks-tpc-di) and
remain under their respective upstream licenses; DIGen itself is TPC-licensed and is fetched
at build time, never vendored here.
