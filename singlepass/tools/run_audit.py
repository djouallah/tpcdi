"""TPC-DI end-state audit — the single-pass-valid subset of Appendix-A.

Ports the checks from `shannon-barrow/databricks-tpc-di`
`src/incremental_batches/audit_validation/automated_audit.sql` that are
meaningful for THIS port's single-pass load (one `dbt run` builds the whole
SCD2 warehouse from Batch1 + Batch2/3 at once — there are no between-batch
checkpoints). It loads the PDGF answer keys (the `*_audit.csv` files that ride
along in the seed) into an `Audit` table and validates the finished warehouse
Delta tables against them plus a set of structural invariants.

Read-only: connects through duckrun with ``read_only=True`` exactly like
scripts/run_queries.py, so it can run against the live warehouse independently
of the ETL. The `Audit` answer-key table is a DuckDB TEMP table (never touches
Delta).

Local:
    WAREHOUSE_PATH=./warehouse TPCDI_DIR=./staging python tools/run_audit.py
OneLake:
    WAREHOUSE_PATH=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Tables \\
      TPCDI_DIR=abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>/Files/tpcdi/sf3 \\
      ONELAKE_TOKEN=... python tools/run_audit.py

Each check prints one line: ``PASS/FAIL/WARN | test | batch | expected | actual``.
Exit status is nonzero iff any FAIL. WARN never fails the run.

-----------------------------------------------------------------------------
Checks that were DELIBERATELY SKIPPED (invalid for a single-pass load — see the
README "Not yet covered" section). Every one of these reads the `DImessages`
table or the Audit meta-rows, neither of which a single-pass load produces:
  * 'Audit table batches' / 'Audit table sources'  — meta-checks on the answer
    keys themselves, not on the warehouse.
  * 'DImessages validation reports' / 'DImessages batches' /
    'DImessages Phase complete records' / 'DImessages sources' /
    'DImessages initial condition'                  — require the DImessages log.
  * Every per-table "row count" check whose ACTUAL comes from
    `sum(MessageData) ... from dimessages` (DimSecurity, DimCompany, DimTrade,
    FactWatches, Financial, FactMarketHistory, FactHoldings row counts, plus
    'FactWatches active watches', 'DimCustomer inactive customers',
    'DimCustomer age range alerts', 'DimCustomer customer tier alerts',
    'DimTrade commission alerts', 'DimTrade charge alerts') — no DImessages log,
    so no per-batch checkpoint row counts to compare against.
  * 'Batch row count' / 'Batch joined row count' /
    'Data visibility row counts' / 'Data visibility joined row counts' — all
    driven off DImessages Visibility/Row-count messages.
Where the upstream check reads BOTH DImessages and the Audit table, we KEEP a
warehouse-only variant (e.g. the row-count checks are re-expressed as a
minimum: actual warehouse count >= the Audit source counts).

Checks tagged WARN (kept but not fatal) are the ones whose correctness depends
on per-batch SOURCE attribution or on the Audit 'Batch' FirstDay/LastDay date
windows — both ambiguous under a single-pass load, where a record's batchid is
the batch that sourced it rather than a discrete load checkpoint.
"""
from __future__ import annotations

import argparse
import os
import sys

import duckrun

HERE = os.path.dirname(os.path.abspath(__file__))
PROJ = os.path.dirname(HERE)

EOT = "DATE '9999-12-31'"  # end of time

# Standard PDGF audit-file schema (positional). DataSet, BatchID, Date, Attribute, Value, DValue.
AUDIT_COLS = ["dataset", "batchid", "date", "attribute", "value", "dvalue"]

# ---------------------------------------------------------------------------
# Checks. Each is (name, severity, pred, sql).
#   pred: 'EQ'   -> actual == expected
#         'GE'   -> actual >= expected  (minimum row counts)
#         'ZERO' -> actual == 0         (violation counts; expected shown as 0)
#   sql : SELECT returning rows with columns (batch, expected, actual).
#         batch may be NULL for whole-table checks.
# Warehouse tables are referenced bare (resolved in the attached catalog set
# current via USE). The answer keys are the TEMP table `Audit`.
# ---------------------------------------------------------------------------


def _zero(name, sql_body, severity="FAIL", batch="NULL::INT"):
    """A 'violations must be 0' check. sql_body is a scalar count expression."""
    return (name, severity, "ZERO", f"SELECT {batch} AS batch, 0 AS expected, ({sql_body}) AS actual")


def _eq(name, expected, actual, severity="FAIL", batch="NULL::INT"):
    return (name, severity, "EQ", f"SELECT {batch} AS batch, ({expected}) AS expected, ({actual}) AS actual")


def _audit_val(dataset, attribute, batch=None):
    w = f"DataSet = '{dataset}' AND Attribute = '{attribute}'"
    if batch is not None:
        w += f" AND BatchID = {batch}"
    return f"SELECT sum(Value) FROM Audit WHERE {w}"


CHECKS = []

# ===== DimBroker =====
CHECKS += [
    _eq("DimBroker row count", _audit_val("DimBroker", "HR_BROKERS"),
        "SELECT count(*) FROM DimBroker"),
    _eq("DimBroker distinct keys", "SELECT count(*) FROM DimBroker",
        "SELECT count(DISTINCT sk_brokerid) FROM DimBroker"),
    _zero("DimBroker BatchID", "SELECT count(*) FROM DimBroker WHERE batchid <> 1"),
    _zero("DimBroker IsCurrent", "SELECT count(*) FROM DimBroker WHERE NOT iscurrent"),
    _zero("DimBroker EndDate", f"SELECT count(*) FROM DimBroker WHERE enddate <> {EOT}"),
    # EffectiveDate is Batch1's batch date; our port uses the first calendar day. WARN: seed-dependent.
    _zero("DimBroker EffectiveDate",
          "SELECT count(*) FROM DimBroker WHERE effectivedate <> (SELECT min(datevalue) FROM DimDate)",
          severity="WARN"),
]

# ===== DimAccount =====
CHECKS += [
    _eq("DimAccount distinct keys", "SELECT count(*) FROM DimAccount",
        "SELECT count(DISTINCT sk_accountid) FROM DimAccount"),
    _eq("DimAccount EndDate", "SELECT count(*) FROM DimAccount",
        f"""SELECT (SELECT count(*) FROM DimAccount a JOIN DimAccount b
                    ON a.accountid = b.accountid AND a.enddate = b.effectivedate)
                 + (SELECT count(*) FROM DimAccount WHERE enddate = {EOT})"""),
    _zero("DimAccount Overlap",
          """SELECT count(*) FROM DimAccount a JOIN DimAccount b ON a.accountid = b.accountid
             AND a.sk_accountid <> b.sk_accountid
             AND a.effectivedate >= b.effectivedate AND a.effectivedate < b.enddate"""),
    _eq("DimAccount End of Time", "SELECT count(DISTINCT accountid) FROM DimAccount",
        f"SELECT count(*) FROM DimAccount WHERE enddate = {EOT}"),
    _zero("DimAccount consolidation", "SELECT count(*) FROM DimAccount WHERE effectivedate = enddate"),
    _eq("DimAccount IsCurrent", "SELECT count(*) FROM DimAccount",
        f"""SELECT (SELECT count(*) FROM DimAccount WHERE enddate = {EOT} AND iscurrent)
                 + (SELECT count(*) FROM DimAccount WHERE enddate < {EOT} AND NOT iscurrent)"""),
    _zero("DimAccount Status", "SELECT count(*) FROM DimAccount WHERE status NOT IN ('Active','Inactive')"),
    _zero("DimAccount TaxStatus",
          "SELECT count(*) FROM DimAccount WHERE batchid = 1 AND taxstatus NOT IN (0,1,2)"),
    _eq("DimAccount SK_CustomerID", "SELECT count(*) FROM DimAccount",
        """SELECT count(*) FROM DimAccount a JOIN DimCustomer c ON a.sk_customerid = c.sk_customerid
           AND c.effectivedate <= a.effectivedate AND a.enddate <= c.enddate"""),
    _eq("DimAccount SK_BrokerID", "SELECT count(*) FROM DimAccount",
        """SELECT count(*) FROM DimAccount a JOIN DimBroker c ON a.sk_brokerid = c.sk_brokerid
           AND c.effectivedate <= a.effectivedate AND a.enddate <= c.enddate"""),
    _zero("DimAccount inactive customers",
          """SELECT count(*) FROM (
                SELECT c.sk_customerid FROM (SELECT * FROM DimCustomer WHERE status = 'Inactive') c
                LEFT JOIN DimAccount a ON a.sk_customerid = c.sk_customerid
                WHERE a.status = 'Inactive' GROUP BY c.sk_customerid HAVING count(*) < 1)"""),
    # batches: 3 distinct batchids, max 3. WARN (single-pass source attribution).
    _zero("DimAccount batches",
          "SELECT CASE WHEN count(DISTINCT batchid) = 3 AND max(batchid) = 3 THEN 0 ELSE 1 END FROM DimAccount",
          severity="WARN"),
    # Row-count minimum vs Audit source counts, per batch. WARN.
    ("DimAccount row count", "WARN", "GE", """
        SELECT a.BatchID AS batch,
          coalesce((SELECT sum(Value) FROM Audit WHERE DataSet='DimAccount' AND Attribute='CA_ADDACCT'  AND BatchID=a.BatchID),0)
        + coalesce((SELECT sum(Value) FROM Audit WHERE DataSet='DimAccount' AND Attribute='CA_CLOSEACCT' AND BatchID=a.BatchID),0)
        + coalesce((SELECT sum(Value) FROM Audit WHERE DataSet='DimAccount' AND Attribute='CA_UPDACCT'  AND BatchID=a.BatchID),0)
        - coalesce((SELECT sum(Value) FROM Audit WHERE DataSet='DimAccount' AND Attribute='CA_ID_HIST'  AND BatchID=a.BatchID),0) AS expected,
          (SELECT count(*) FROM DimAccount WHERE batchid = a.BatchID) AS actual
        FROM (SELECT DISTINCT BatchID FROM Audit WHERE BatchID IN (1,2,3)) a"""),
]

# ===== DimCustomer =====
CHECKS += [
    _eq("DimCustomer distinct keys", "SELECT count(*) FROM DimCustomer",
        "SELECT count(DISTINCT sk_customerid) FROM DimCustomer"),
    _eq("DimCustomer EndDate", "SELECT count(*) FROM DimCustomer",
        f"""SELECT (SELECT count(*) FROM DimCustomer a JOIN DimCustomer b
                    ON a.customerid = b.customerid AND a.enddate = b.effectivedate)
                 + (SELECT count(*) FROM DimCustomer WHERE enddate = {EOT})"""),
    _zero("DimCustomer Overlap",
          """SELECT count(*) FROM DimCustomer a JOIN DimCustomer b ON a.customerid = b.customerid
             AND a.sk_customerid <> b.sk_customerid
             AND a.effectivedate >= b.effectivedate AND a.effectivedate < b.enddate"""),
    _eq("DimCustomer End of Time", "SELECT count(DISTINCT customerid) FROM DimCustomer",
        f"SELECT count(*) FROM DimCustomer WHERE enddate = {EOT}"),
    _zero("DimCustomer consolidation", "SELECT count(*) FROM DimCustomer WHERE effectivedate = enddate"),
    _eq("DimCustomer IsCurrent", "SELECT count(*) FROM DimCustomer",
        f"""SELECT (SELECT count(*) FROM DimCustomer WHERE enddate = {EOT} AND iscurrent)
                 + (SELECT count(*) FROM DimCustomer WHERE enddate < {EOT} AND NOT iscurrent)"""),
    _zero("DimCustomer Status", "SELECT count(*) FROM DimCustomer WHERE status NOT IN ('Active','Inactive')"),
    _zero("DimCustomer Gender", "SELECT count(*) FROM DimCustomer WHERE gender NOT IN ('M','F','U')"),
    _zero("DimCustomer TaxID",
          "SELECT count(*) FROM DimCustomer WHERE taxid NOT LIKE '___-__-____'"),
    _zero("DimCustomer Phone1",
          """SELECT count(*) FROM DimCustomer WHERE phone1 NOT LIKE '+1 (___) ___-____%'
             AND phone1 NOT LIKE '(___) ___-____%' AND phone1 NOT LIKE '___-____%'
             AND phone1 <> '' AND phone1 IS NOT NULL"""),
    _zero("DimCustomer Phone2",
          """SELECT count(*) FROM DimCustomer WHERE phone2 NOT LIKE '+1 (___) ___-____%'
             AND phone2 NOT LIKE '(___) ___-____%' AND phone2 NOT LIKE '___-____%'
             AND phone2 <> '' AND phone2 IS NOT NULL"""),
    _zero("DimCustomer Phone3",
          """SELECT count(*) FROM DimCustomer WHERE phone3 NOT LIKE '+1 (___) ___-____%'
             AND phone3 NOT LIKE '(___) ___-____%' AND phone3 NOT LIKE '___-____%'
             AND phone3 <> '' AND phone3 IS NOT NULL"""),
    _zero("DimCustomer Email1",
          "SELECT count(*) FROM DimCustomer WHERE email1 NOT LIKE '_%.%@%.%' AND email1 IS NOT NULL"),
    _zero("DimCustomer Email2",
          "SELECT count(*) FROM DimCustomer WHERE email2 NOT LIKE '_%.%@%.%' AND email2 <> '' AND email2 IS NOT NULL"),
    _eq("DimCustomer LocalTaxRate", "SELECT count(*) FROM DimCustomer",
        """SELECT count(*) FROM DimCustomer c JOIN TaxRate t
           ON c.localtaxratedesc = t.tx_name AND c.localtaxrate = t.tx_rate"""),
    _eq("DimCustomer NationalTaxRate", "SELECT count(*) FROM DimCustomer",
        """SELECT count(*) FROM DimCustomer c JOIN TaxRate t
           ON c.nationaltaxratedesc = t.tx_name AND c.nationaltaxrate = t.tx_rate"""),
    # For current customers matching a Prospect, the demographic fields must match too.
    _eq("DimCustomer demographic fields",
        "SELECT count(*) FROM DimCustomer WHERE agencyid IS NOT NULL AND iscurrent",
        """SELECT count(*) FROM DimCustomer c JOIN Prospect p
           ON upper(c.firstname||c.lastname||c.addressline1||coalesce(c.addressline2,'')||c.postalcode)
            = upper(p.firstname||p.lastname||p.addressline1||coalesce(p.addressline2,'')||p.postalcode)
           AND coalesce(c.creditrating,0) = coalesce(p.creditrating,0)
           AND coalesce(c.networth,0) = coalesce(p.networth,0)
           AND coalesce(c.marketingnameplate,'') = coalesce(p.marketingnameplate,'')
           AND c.iscurrent""", severity="WARN"),
    _zero("DimCustomer batches",
          "SELECT CASE WHEN count(DISTINCT batchid) = 3 AND max(batchid) = 3 THEN 0 ELSE 1 END FROM DimCustomer",
          severity="WARN"),
    ("DimCustomer row count", "WARN", "GE", """
        SELECT a.BatchID AS batch,
          coalesce((SELECT sum(Value) FROM Audit WHERE DataSet='DimCustomer' AND Attribute='C_NEW'    AND BatchID=a.BatchID),0)
        + coalesce((SELECT sum(Value) FROM Audit WHERE DataSet='DimCustomer' AND Attribute='C_INACT'  AND BatchID=a.BatchID),0)
        + coalesce((SELECT sum(Value) FROM Audit WHERE DataSet='DimCustomer' AND Attribute='C_UPDCUST' AND BatchID=a.BatchID),0)
        - coalesce((SELECT sum(Value) FROM Audit WHERE DataSet='DimCustomer' AND Attribute='C_ID_HIST' AND BatchID=a.BatchID),0) AS expected,
          (SELECT count(*) FROM DimCustomer WHERE batchid = a.BatchID) AS actual
        FROM (SELECT DISTINCT BatchID FROM Audit WHERE BatchID IN (1,2,3)) a"""),
]

# ===== DimSecurity =====
CHECKS += [
    _eq("DimSecurity distinct keys", "SELECT count(*) FROM DimSecurity",
        "SELECT count(DISTINCT sk_securityid) FROM DimSecurity"),
    _eq("DimSecurity EndDate", "SELECT count(*) FROM DimSecurity",
        f"""SELECT (SELECT count(*) FROM DimSecurity a JOIN DimSecurity b
                    ON a.symbol = b.symbol AND a.enddate = b.effectivedate)
                 + (SELECT count(*) FROM DimSecurity WHERE enddate = {EOT})"""),
    _zero("DimSecurity Overlap",
          """SELECT count(*) FROM DimSecurity a JOIN DimSecurity b ON a.symbol = b.symbol
             AND a.sk_securityid <> b.sk_securityid
             AND a.effectivedate >= b.effectivedate AND a.effectivedate < b.enddate"""),
    _eq("DimSecurity End of Time", "SELECT count(DISTINCT symbol) FROM DimSecurity",
        f"SELECT count(*) FROM DimSecurity WHERE enddate = {EOT}"),
    _zero("DimSecurity consolidation", "SELECT count(*) FROM DimSecurity WHERE effectivedate = enddate"),
    _zero("DimSecurity batches", "SELECT count(*) FROM DimSecurity WHERE batchid <> 1"),
    _eq("DimSecurity IsCurrent", "SELECT count(*) FROM DimSecurity",
        f"""SELECT (SELECT count(*) FROM DimSecurity WHERE enddate = {EOT} AND iscurrent)
                 + (SELECT count(*) FROM DimSecurity WHERE enddate < {EOT} AND NOT iscurrent)"""),
    _zero("DimSecurity Status", "SELECT count(*) FROM DimSecurity WHERE status NOT IN ('Active','Inactive')"),
    _eq("DimSecurity SK_CompanyID", "SELECT count(*) FROM DimSecurity",
        """SELECT count(*) FROM DimSecurity a JOIN DimCompany c ON a.sk_companyid = c.sk_companyid
           AND c.effectivedate <= a.effectivedate AND a.enddate <= c.enddate"""),
    _zero("DimSecurity ExchangeID",
          "SELECT count(*) FROM DimSecurity WHERE exchangeid NOT IN ('NYSE','NASDAQ','AMEX','PCX')"),
    _zero("DimSecurity Issue",
          "SELECT count(*) FROM DimSecurity WHERE issue NOT IN ('COMMON','PREF_A','PREF_B','PREF_C','PREF_D')"),
]

# ===== DimCompany =====
CHECKS += [
    _eq("DimCompany distinct keys", "SELECT count(*) FROM DimCompany",
        "SELECT count(DISTINCT sk_companyid) FROM DimCompany"),
    _eq("DimCompany EndDate", "SELECT count(*) FROM DimCompany",
        f"""SELECT (SELECT count(*) FROM DimCompany a JOIN DimCompany b
                    ON a.companyid = b.companyid AND a.enddate = b.effectivedate)
                 + (SELECT count(*) FROM DimCompany WHERE enddate = {EOT})"""),
    _zero("DimCompany Overlap",
          """SELECT count(*) FROM DimCompany a JOIN DimCompany b ON a.companyid = b.companyid
             AND a.sk_companyid <> b.sk_companyid
             AND a.effectivedate >= b.effectivedate AND a.effectivedate < b.enddate"""),
    _eq("DimCompany End of Time", "SELECT count(DISTINCT companyid) FROM DimCompany",
        f"SELECT count(*) FROM DimCompany WHERE enddate = {EOT}"),
    _zero("DimCompany consolidation", "SELECT count(*) FROM DimCompany WHERE effectivedate = enddate"),
    _zero("DimCompany batches", "SELECT count(*) FROM DimCompany WHERE batchid <> 1"),
    _zero("DimCompany Status", "SELECT count(*) FROM DimCompany WHERE status NOT IN ('Active','Inactive')"),
    _zero("DimCompany distinct names",
          "SELECT count(*) FROM DimCompany a JOIN DimCompany b ON a.name = b.name AND a.companyid <> b.companyid"),
    _zero("DimCompany Industry",
          "SELECT count(*) FROM DimCompany WHERE industry NOT IN (SELECT DISTINCT in_name FROM Industry)"),
    _zero("DimCompany SPrating",
          """SELECT count(*) FROM DimCompany WHERE sprating NOT IN
             ('AAA','AA','A','BBB','BB','B','CCC','CC','C','D','AA+','A+','BBB+','BB+','B+','CCC+',
              'AA-','A-','BBB-','BB-','B-','CCC-') AND sprating IS NOT NULL"""),
    _zero("DimCompany Country",
          """SELECT count(*) FROM DimCompany
             WHERE country NOT IN ('Canada','United States of America','') AND country IS NOT NULL"""),
]

# ===== Prospect =====
CHECKS += [
    _zero("Prospect SK_UpdateDateID",
          "SELECT count(*) FROM Prospect WHERE sk_recorddateid < sk_updatedateid"),
    _zero("Prospect batches",
          "SELECT CASE WHEN count(DISTINCT batchid) = 3 AND max(batchid) = 3 THEN 0 ELSE 1 END FROM Prospect",
          severity="WARN"),
    _zero("Prospect Country",
          """SELECT count(*) FROM Prospect
             WHERE country NOT IN ('Canada','United States of America') AND country IS NOT NULL"""),
    # Recompute the marketingnameplate tags from the raw fields and compare to the stored value.
    # income is VARCHAR in the source, so cast it; networth is numeric.
    _zero("Prospect MarketingNameplate", """
        SELECT sum(CASE WHEN (coalesce(networth,0) > 1000000 OR coalesce(try_cast(income AS double),0) > 200000)
                        AND marketingnameplate NOT LIKE '%HighValue%' THEN 1 ELSE 0 END)
             + sum(CASE WHEN (coalesce(numberchildren,0) > 3 OR coalesce(numbercreditcards,0) > 5)
                        AND marketingnameplate NOT LIKE '%Expenses%' THEN 1 ELSE 0 END)
             + sum(CASE WHEN coalesce(age,0) > 45 AND marketingnameplate NOT LIKE '%Boomer%' THEN 1 ELSE 0 END)
             + sum(CASE WHEN (coalesce(try_cast(income AS double),50000) < 50000 OR coalesce(creditrating,600) < 600
                             OR coalesce(networth,100000) < 100000)
                        AND marketingnameplate NOT LIKE '%MoneyAlert%' THEN 1 ELSE 0 END)
             + sum(CASE WHEN (coalesce(numbercars,0) > 3 OR coalesce(numbercreditcards,0) > 7)
                        AND marketingnameplate NOT LIKE '%Spender%' THEN 1 ELSE 0 END)
             + sum(CASE WHEN (coalesce(age,25) < 25 AND coalesce(networth,0) > 1000000)
                        AND marketingnameplate NOT LIKE '%Inherited%' THEN 1 ELSE 0 END)
        FROM Prospect"""),
]

# ===== FactWatches =====
CHECKS += [
    _zero("FactWatches batches",
          "SELECT CASE WHEN count(DISTINCT batchid) = 3 AND max(batchid) = 3 THEN 0 ELSE 1 END FROM FactWatches",
          severity="WARN"),
    _eq("FactWatches SK_CustomerID", "SELECT count(*) FROM FactWatches",
        """SELECT count(*) FROM FactWatches a
           JOIN DimDate _d ON _d.sk_dateid = a.sk_dateid_dateplaced
           JOIN DimCustomer c ON a.sk_customerid = c.sk_customerid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
    _eq("FactWatches SK_SecurityID", "SELECT count(*) FROM FactWatches",
        """SELECT count(*) FROM FactWatches a
           JOIN DimDate _d ON _d.sk_dateid = a.sk_dateid_dateplaced
           JOIN DimSecurity c ON a.sk_securityid = c.sk_securityid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
]

# ===== DimTrade =====
CHECKS += [
    _eq("DimTrade canceled trades",
        _audit_val("DimTrade", "T_CanceledTrades"),
        "SELECT count(*) FROM DimTrade WHERE status = 'Canceled'"),
    _zero("DimTrade batches",
          "SELECT CASE WHEN count(DISTINCT batchid) = 3 AND max(batchid) = 3 THEN 0 ELSE 1 END FROM DimTrade",
          severity="WARN"),
    _eq("DimTrade distinct keys", "SELECT count(*) FROM DimTrade",
        "SELECT count(DISTINCT tradeid) FROM DimTrade"),
    _eq("DimTrade SK_BrokerID", "SELECT count(*) FROM DimTrade WHERE sk_brokerid IS NOT NULL",
        """SELECT count(*) FROM DimTrade a JOIN DimDate _d ON _d.sk_dateid = a.sk_createdateid
           JOIN DimBroker c ON a.sk_brokerid = c.sk_brokerid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
    _eq("DimTrade SK_CompanyID", "SELECT count(*) FROM DimTrade",
        """SELECT count(*) FROM DimTrade a JOIN DimDate _d ON _d.sk_dateid = a.sk_createdateid
           JOIN DimCompany c ON a.sk_companyid = c.sk_companyid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
    _eq("DimTrade SK_SecurityID", "SELECT count(*) FROM DimTrade",
        """SELECT count(*) FROM DimTrade a JOIN DimDate _d ON _d.sk_dateid = a.sk_createdateid
           JOIN DimSecurity c ON a.sk_securityid = c.sk_securityid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
    _eq("DimTrade SK_CustomerID", "SELECT count(*) FROM DimTrade",
        """SELECT count(*) FROM DimTrade a JOIN DimDate _d ON _d.sk_dateid = a.sk_createdateid
           JOIN DimCustomer c ON a.sk_customerid = c.sk_customerid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
    _eq("DimTrade SK_AccountID", "SELECT count(*) FROM DimTrade",
        """SELECT count(*) FROM DimTrade a JOIN DimDate _d ON _d.sk_dateid = a.sk_createdateid
           JOIN DimAccount c ON a.sk_accountid = c.sk_accountid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
    _zero("DimTrade Status",
          "SELECT count(*) FROM DimTrade WHERE status NOT IN ('Canceled','Pending','Submitted','Active','Completed')"),
    _zero("DimTrade Type",
          "SELECT count(*) FROM DimTrade WHERE type NOT IN ('Market Buy','Market Sell','Stop Loss','Limit Sell','Limit Buy')"),
]

# ===== Financial =====
CHECKS += [
    _eq("Financial SK_CompanyID", "SELECT count(*) FROM Financial",
        "SELECT count(*) FROM Financial a JOIN DimCompany c ON a.sk_companyid = c.sk_companyid"),
    # FI_YEAR within Batch1's window — depends on the Audit 'Batch' rows. WARN.
    _zero("Financial FI_YEAR", """
        SELECT (SELECT count(*) FROM Financial WHERE fi_year < extract(year FROM
                  (SELECT min(Date) FROM Audit WHERE DataSet='Batch' AND BatchID=1 AND Attribute='FirstDay')))
             + (SELECT count(*) FROM Financial WHERE fi_year > extract(year FROM
                  (SELECT max(Date) FROM Audit WHERE DataSet='Batch' AND BatchID=1 AND Attribute='LastDay')))""",
          severity="WARN"),
    _zero("Financial FI_QTR", "SELECT count(*) FROM Financial WHERE fi_qtr NOT IN (1,2,3,4)"),
    _zero("Financial FI_QTR_START_DATE",
          """SELECT count(*) FROM Financial
             WHERE fi_year <> extract(year FROM fi_qtr_start_date)
                OR extract(month FROM fi_qtr_start_date) <> (fi_qtr - 1) * 3 + 1
                OR extract(day FROM fi_qtr_start_date) <> 1"""),
    _zero("Financial EPS",
          """SELECT count(*) FROM Financial
             WHERE round(fi_net_earn / nullif(fi_out_basic,0), 2) - fi_basic_eps NOT BETWEEN -0.4 AND 0.4
                OR round(fi_net_earn / nullif(fi_out_dilut,0), 2) - fi_dilut_eps NOT BETWEEN -0.4 AND 0.4
                OR round(fi_net_earn / nullif(fi_revenue,0), 2)   - fi_margin    NOT BETWEEN -0.4 AND 0.4"""),
]

# ===== FactMarketHistory =====
CHECKS += [
    _zero("FactMarketHistory batches",
          "SELECT CASE WHEN count(DISTINCT batchid) = 3 AND max(batchid) = 3 THEN 0 ELSE 1 END FROM FactMarketHistory",
          severity="WARN"),
    _eq("FactMarketHistory SK_CompanyID", "SELECT count(*) FROM FactMarketHistory",
        """SELECT count(*) FROM FactMarketHistory a JOIN DimDate _d ON _d.sk_dateid = a.sk_dateid
           JOIN DimCompany c ON a.sk_companyid = c.sk_companyid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
    _eq("FactMarketHistory SK_SecurityID", "SELECT count(*) FROM FactMarketHistory",
        """SELECT count(*) FROM FactMarketHistory a JOIN DimDate _d ON _d.sk_dateid = a.sk_dateid
           JOIN DimSecurity c ON a.sk_securityid = c.sk_securityid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
    # THE check that the FactMarketHistory 52-week fix restores:
    _zero("FactMarketHistory relative dates",
          """SELECT count(*) FROM FactMarketHistory
             WHERE fiftytwoweeklow > daylow OR daylow > closeprice
                OR closeprice > dayhigh OR dayhigh > fiftytwoweekhigh"""),
]

# ===== FactHoldings =====
CHECKS += [
    _zero("FactHoldings batches",
          "SELECT CASE WHEN count(DISTINCT batchid) = 3 AND max(batchid) = 3 THEN 0 ELSE 1 END FROM FactHoldings",
          severity="WARN"),
    _eq("FactHoldings SK_CustomerID", "SELECT count(*) FROM FactHoldings",
        """SELECT count(*) FROM FactHoldings a JOIN DimDate _d ON _d.sk_dateid = a.sk_dateid
           JOIN DimCustomer c ON a.sk_customerid = c.sk_customerid AND c.effectivedate <= _d.datevalue"""),
    _eq("FactHoldings SK_AccountID", "SELECT count(*) FROM FactHoldings",
        """SELECT count(*) FROM FactHoldings a JOIN DimDate _d ON _d.sk_dateid = a.sk_dateid
           JOIN DimAccount c ON a.sk_accountid = c.sk_accountid AND c.effectivedate <= _d.datevalue"""),
    _eq("FactHoldings CurrentTradeID", "SELECT count(*) FROM FactHoldings",
        """SELECT count(*) FROM FactHoldings a JOIN DimTrade t ON a.currenttradeid = t.tradeid
           AND a.sk_dateid = t.sk_closedateid AND a.sk_timeid = t.sk_closetimeid"""),
]

# ===== FactCashBalances =====
CHECKS += [
    _zero("FactCashBalances batches",
          "SELECT CASE WHEN count(DISTINCT batchid) = 3 AND max(batchid) = 3 THEN 0 ELSE 1 END FROM FactCashBalances",
          severity="WARN"),
    _eq("FactCashBalances SK_CustomerID", "SELECT count(*) FROM FactCashBalances",
        """SELECT count(*) FROM FactCashBalances a JOIN DimDate _d ON _d.sk_dateid = a.sk_dateid
           JOIN DimCustomer c ON a.sk_customerid = c.sk_customerid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
    _eq("FactCashBalances SK_AccountID", "SELECT count(*) FROM FactCashBalances",
        """SELECT count(*) FROM FactCashBalances a JOIN DimDate _d ON _d.sk_dateid = a.sk_dateid
           JOIN DimAccount c ON a.sk_accountid = c.sk_accountid
             AND c.effectivedate <= _d.datevalue AND _d.datevalue <= c.enddate"""),
]


# ---------------------------------------------------------------------------
def load_audit(raw_con, seed):
    """Create the TEMP `Audit` table and load every Batch*/*_audit.csv answer key.

    Robust to a header row (we drop any row whose BatchID isn't an integer) and we
    peek the first file to confirm the column order matches the standard PDGF layout
    (DataSet, BatchID, Date, Attribute, Value, DValue), warning loudly if it doesn't.
    """
    glob = f"{str(seed).rstrip('/')}/Batch*/*_audit.csv"
    read = (f"read_csv('{glob}', header=false, all_varchar=true, delim=',', "
            "quote='', escape='', nullstr='', null_padding=true, filename=false")
    # Probe: read the first physical row as raw strings to check header/order.
    try:
        probe = raw_con.execute(f"SELECT * FROM {read}) LIMIT 1").fetchone()
    except Exception as e:
        sys.exit(f"ERROR: could not read audit files at {glob}: {e}")
    if probe is None:
        sys.exit(f"ERROR: no audit rows found at {glob}")
    ncols = len(probe)
    if ncols < 6:
        sys.exit(f"ERROR: audit file has {ncols} columns, expected >= 6 at {glob}: {probe}")
    # Header check: if the first row looks like a header, verify its names match the assumed order.
    first = [(" " if v is None else str(v)).strip().lower() for v in probe[:6]]
    if first[0] == "dataset" or not _looks_int(probe[1]):
        expected = ["dataset", "batchid", "date", "attribute", "value", "dvalue"]
        if first != expected:
            print(f"  WARNING: audit header {first} != assumed order {expected}; "
                  "loading positionally anyway — verify the column mapping.")

    named = ", ".join(f"column{i} AS {c}" for i, c in enumerate(AUDIT_COLS))
    raw_con.execute(
        'CREATE TEMP TABLE Audit ("dataset" VARCHAR, "batchid" INT, "date" DATE, '
        '"attribute" VARCHAR, "value" BIGINT, "dvalue" DOUBLE)')
    # Cast in SQL; the WHERE drops a header row (and any non-numeric junk) uniformly.
    raw_con.execute(f"""
        INSERT INTO Audit
        SELECT column0,
               try_cast(column1 AS INT),
               try_cast(column2 AS DATE),
               column3,
               try_cast(column4 AS BIGINT),
               try_cast(column5 AS DOUBLE)
        FROM {read})
        WHERE try_cast(column1 AS INT) IS NOT NULL
    """)
    n = raw_con.execute("SELECT count(*) FROM Audit").fetchone()[0]
    if n == 0:
        sys.exit(f"ERROR: Audit table is empty after loading {glob}")
    print(f"  loaded {n:,} audit rows from {glob}")


def _looks_int(v):
    try:
        int(str(v).strip())
        return True
    except (TypeError, ValueError):
        return False


def run_checks(raw_con):
    print(f"\n  {'status':<6} {'test':<34}{'batch':>6}{'expected':>16}{'actual':>16}")
    print("  " + "-" * 78)
    any_fail = False
    n_pass = n_warn = n_fail = n_err = 0
    for name, severity, pred, sql in CHECKS:
        try:
            rows = raw_con.execute(sql).fetchall()
        except Exception as e:  # a broken check reports as an error, never silently passes
            n_err += 1
            any_fail = True
            print(f"  {'ERROR':<6} {name:<34}{'':>6}{'':>16}{'':>16}  {str(e).splitlines()[0][:60]}")
            continue
        for row in rows:
            batch, expected, actual = row[0], row[1], row[2]
            if pred == "EQ":
                ok = expected == actual
            elif pred == "GE":
                ok = (actual or 0) >= (expected or 0)
            else:  # ZERO
                ok = (actual or 0) == 0
            status = "PASS" if ok else severity
            if ok:
                n_pass += 1
            elif severity == "WARN":
                n_warn += 1
            else:
                n_fail += 1
                any_fail = True
            b = "" if batch is None else str(batch)
            exp = "" if expected is None else f"{expected:,}" if isinstance(expected, int) else str(expected)
            act = "" if actual is None else f"{actual:,}" if isinstance(actual, int) else str(actual)
            print(f"  {status:<6} {name:<34}{b:>6}{exp:>16}{act:>16}")
    print("  " + "-" * 78)
    print(f"  {n_pass} passed, {n_warn} warned, {n_fail} failed"
          + (f", {n_err} errored" if n_err else ""))
    return any_fail


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default=os.environ.get("WAREHOUSE_PATH"),
                    help="root_path of the Delta warehouse (local dir or abfss://.../Tables)")
    ap.add_argument("--schema", default=os.environ.get("DBT_SCHEMA", "tpcdi"))
    ap.add_argument("--seed", default=os.environ.get("TPCDI_DIR"),
                    help="seed dir holding Batch*/ with the *_audit.csv answer keys "
                         "(local dir or abfss://.../Files/tpcdi/sf<N>)")
    args = ap.parse_args()

    if not args.warehouse:
        sys.exit("ERROR: set --warehouse or WAREHOUSE_PATH (local dir or abfss://.../Tables)")
    if not args.seed:
        sys.exit("ERROR: set --seed or TPCDI_DIR (dir with Batch*/*_audit.csv)")

    storage_options = None
    token = os.environ.get("ONELAKE_TOKEN", "")
    if args.warehouse.startswith("abfss://"):
        if not token:
            sys.exit("ERROR: ONELAKE_TOKEN is empty — needed to query an abfss:// warehouse")
        storage_options = {"bearer_token": token}

    conn = duckrun.connect(
        args.warehouse, storage_options=storage_options, schema=args.schema, read_only=True)
    # Raw DuckDB connection: we hold the answer keys in a TEMP table and run the checks here to
    # bypass duckrun's Delta-DML classifier (a plain CREATE TABLE in the primary Delta catalog would
    # be treated as a Delta overwrite). duckrun's primary catalog already minted an UNSCOPED Azure
    # secret (and installed the azure extension + transport), which covers the whole storage account
    # — so read_csv over the seed's Files/ path authenticates with no extra secret.
    raw = conn.con

    print(f"\n  warehouse: {args.warehouse} (schema {args.schema})")
    print(f"  seed:      {args.seed}")

    if str(args.seed).startswith("abfss://") and not token:
        sys.exit("ERROR: ONELAKE_TOKEN is empty — needed to read abfss:// audit files")

    load_audit(raw, args.seed)
    any_fail = run_checks(raw)

    if any_fail:
        sys.exit("\n  AUDIT FAILED — see FAIL/ERROR lines above.")
    print("\n  audit passed (WARN lines are non-fatal).")


if __name__ == "__main__":
    main()
