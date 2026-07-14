-- Ported from shannon-barrow/databricks-tpc-di
--   src/incremental_batches/audit_validation/batch_validation.sql
-- Writes the per-batch Validation rows to DImessages (run by run.py after each batch):
-- a 'Row count' row per warehouse table, a 'Row count joined' row per fact (referential
-- integrity), and the 'Inactive customers' / 'Inactive watches' rows. 24 rows per batch;
-- the full audit counts and cross-checks them. Dialect: ${...} -> {{batch_id}};
-- CURRENT_TIMESTAMP() -> now(); bare table names (schema set on the connection);
-- MessageData is VARCHAR in DImessages, so the counts are cast; UNION -> UNION ALL (every
-- row has a distinct source/text so there is nothing to dedup).
INSERT INTO DImessages
SELECT
  now()::timestamp AS MessageDateAndTime,
  {{batch_id}}     AS BatchID,
  MessageSource,
  MessageText,
  'Validation'     AS MessageType,
  cast(MessageData AS VARCHAR) AS MessageData
FROM (
  SELECT 'DimAccount'        AS MessageSource, 'Row count' AS MessageText, count(1) AS MessageData FROM DimAccount
  UNION ALL SELECT 'DimBroker',        'Row count', count(1) FROM DimBroker
  UNION ALL SELECT 'DimCompany',       'Row count', count(1) FROM DimCompany
  UNION ALL SELECT 'DimCustomer',      'Row count', count(1) FROM DimCustomer
  UNION ALL SELECT 'DimDate',          'Row count', count(1) FROM DimDate
  UNION ALL SELECT 'DimSecurity',      'Row count', count(1) FROM DimSecurity
  UNION ALL SELECT 'DimTime',          'Row count', count(1) FROM DimTime
  UNION ALL SELECT 'DimTrade',         'Row count', count(1) FROM DimTrade
  UNION ALL SELECT 'FactCashBalances', 'Row count', count(1) FROM FactCashBalances
  UNION ALL SELECT 'FactHoldings',     'Row count', count(1) FROM FactHoldings
  UNION ALL SELECT 'FactMarketHistory','Row count', count(1) FROM FactMarketHistory
  UNION ALL SELECT 'FactWatches',      'Row count', count(1) FROM FactWatches
  UNION ALL SELECT 'Financial',        'Row count', count(1) FROM Financial
  UNION ALL SELECT 'Industry',         'Row count', count(1) FROM Industry
  UNION ALL SELECT 'Prospect',         'Row count', count(1) FROM Prospect
  UNION ALL SELECT 'StatusType',       'Row count', count(1) FROM StatusType
  UNION ALL SELECT 'TaxRate',          'Row count', count(1) FROM TaxRate
  UNION ALL SELECT 'TradeType',        'Row count', count(1) FROM TradeType
  UNION ALL SELECT 'FactCashBalances', 'Row count joined', count(1)
    FROM FactCashBalances f
    JOIN DimAccount a ON f.SK_AccountID = a.SK_AccountID
    JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    JOIN DimBroker b ON a.SK_BrokerID = b.SK_BrokerID
    JOIN DimDate d ON f.SK_DateID = d.SK_DateID
  UNION ALL SELECT 'FactHoldings', 'Row count joined', count(1)
    FROM FactHoldings f
    JOIN DimAccount a ON f.SK_AccountID = a.SK_AccountID
    JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    JOIN DimBroker b ON a.SK_BrokerID = b.SK_BrokerID
    JOIN DimDate d ON f.SK_DateID = d.SK_DateID
    JOIN DimTime t ON f.SK_TimeID = t.SK_TimeID
    JOIN DimCompany m ON f.SK_CompanyID = m.SK_CompanyID
    JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION ALL SELECT 'FactMarketHistory', 'Row count joined', count(1)
    FROM FactMarketHistory f
    JOIN DimDate d ON f.SK_DateID = d.SK_DateID
    JOIN DimCompany m ON f.SK_CompanyID = m.SK_CompanyID
    JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION ALL SELECT 'FactWatches', 'Row count joined', count(1)
    FROM FactWatches f
    JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    JOIN DimDate dp ON f.SK_DateID_DatePlaced = dp.SK_DateID
    JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION ALL SELECT 'DimCustomer', 'Inactive customers', count(1)
    FROM DimCustomer WHERE IsCurrent AND Status = 'Inactive'
  UNION ALL SELECT 'FactWatches', 'Inactive watches', count(1)
    FROM FactWatches WHERE SK_DateID_DateRemoved IS NOT NULL
) AS v
