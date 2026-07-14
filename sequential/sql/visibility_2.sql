-- Data Visibility snapshot #2 (end-of-run). Ported from shannon-barrow/databricks-tpc-di
--   src/incremental_batches/audit_validation/Appendix_v1.1.0/AppendixC/tpcdi_visibility_2.sql
-- Records a full set of table row counts (basic + referential-integrity "joined") into
-- DImessages with MessageType='Visibility_2'. run.py runs this at the very end (sibling of
-- visibility_1.sql, which runs after batch 1); the audit's 'Data visibility row counts' /
-- 'Data visibility joined row counts' checks assert row counts never decrease between the
-- two snapshots (m2.MessageDateAndTime > m1) and that joined == unjoined at a snapshot.
-- Dialect: CURRENT_TIMESTAMP -> now()::timestamp; bare table names (schema on connection);
-- MessageData is VARCHAR in DImessages so counts are cast; UNION -> UNION ALL (every
-- (source,text) pair is unique). now() is constant within the statement, so all rows of
-- one snapshot share a MessageDateAndTime (required by the joined-vs-unjoined check).
-- The classic sets BatchID = max(BatchID) from DImessages; we take it as a {{batch_id}}
-- parameter from the orchestrator (== that max) instead, because duckrun only routes an
-- INSERT to a Delta append when its SELECT does NOT also scan the target table.
INSERT INTO DImessages
SELECT
  now()::timestamp AS MessageDateAndTime,
  {{batch_id}} AS BatchID,
  MessageSource,
  MessageText,
  'Visibility_2' AS MessageType,
  MessageData
FROM (
  SELECT 'DimAccount'        AS MessageSource, 'Row count' AS MessageText, cast(count(*) AS varchar) AS MessageData FROM DimAccount
  UNION ALL SELECT 'DimBroker',   'Row count', cast(count(*) AS varchar) FROM DimBroker
  UNION ALL SELECT 'DimCompany',  'Row count', cast(count(*) AS varchar) FROM DimCompany
  UNION ALL SELECT 'DimCustomer', 'Row count', cast(count(*) AS varchar) FROM DimCustomer
  UNION ALL SELECT 'DimDate',     'Row count', cast(count(*) AS varchar) FROM DimDate
  UNION ALL SELECT 'DimSecurity', 'Row count', cast(count(*) AS varchar) FROM DimSecurity
  UNION ALL SELECT 'DimTime',     'Row count', cast(count(*) AS varchar) FROM DimTime
  UNION ALL SELECT 'DimTrade',    'Row count', cast(count(*) AS varchar) FROM DimTrade
  UNION ALL SELECT 'Financial',   'Row count', cast(count(*) AS varchar) FROM Financial
  UNION ALL SELECT 'Industry',    'Row count', cast(count(*) AS varchar) FROM Industry
  UNION ALL SELECT 'Prospect',    'Row count', cast(count(*) AS varchar) FROM Prospect
  UNION ALL SELECT 'StatusType',  'Row count', cast(count(*) AS varchar) FROM StatusType
  UNION ALL SELECT 'TaxRate',     'Row count', cast(count(*) AS varchar) FROM TaxRate
  UNION ALL SELECT 'TradeType',   'Row count', cast(count(*) AS varchar) FROM TradeType
  UNION ALL SELECT 'FactCashBalances', 'Row count', cast(count(*) AS varchar) FROM FactCashBalances
  UNION ALL SELECT 'FactCashBalances', 'Row count joined', cast(count(*) AS varchar)
    FROM FactCashBalances f
    JOIN DimAccount  a ON f.SK_AccountID  = a.SK_AccountID
    JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    JOIN DimBroker   b ON a.SK_BrokerID   = b.SK_BrokerID
    JOIN DimDate     d ON f.SK_DateID     = d.SK_DateID
  UNION ALL SELECT 'FactHoldings', 'Row count', cast(count(*) AS varchar) FROM FactHoldings
  UNION ALL SELECT 'FactHoldings', 'Row count joined', cast(count(*) AS varchar)
    FROM FactHoldings f
    JOIN DimAccount  a ON f.SK_AccountID  = a.SK_AccountID
    JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    JOIN DimBroker   b ON a.SK_BrokerID   = b.SK_BrokerID
    JOIN DimDate     d ON f.SK_DateID     = d.SK_DateID
    JOIN DimTime     t ON f.SK_TimeID     = t.SK_TimeID
    JOIN DimCompany  m ON f.SK_CompanyID  = m.SK_CompanyID
    JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION ALL SELECT 'FactMarketHistory', 'Row count', cast(count(*) AS varchar) FROM FactMarketHistory
  UNION ALL SELECT 'FactMarketHistory', 'Row count joined', cast(count(*) AS varchar)
    FROM FactMarketHistory f
    JOIN DimDate     d ON f.SK_DateID     = d.SK_DateID
    JOIN DimCompany  m ON f.SK_CompanyID  = m.SK_CompanyID
    JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION ALL SELECT 'FactWatches', 'Row count', cast(count(*) AS varchar) FROM FactWatches
  UNION ALL SELECT 'FactWatches', 'Row count joined', cast(count(*) AS varchar)
    FROM FactWatches f
    JOIN DimCustomer c  ON f.SK_CustomerID = c.SK_CustomerID
    JOIN DimDate     dp ON f.SK_DateID_DatePlaced = dp.SK_DateID
    JOIN DimSecurity s  ON f.SK_SecurityID = s.SK_SecurityID
) y;

