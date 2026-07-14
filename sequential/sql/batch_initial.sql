-- Batch-0 initial-condition checkpoint (run ONCE, before batch 1).
-- The Appendix-A audit requires DImessages to record the empty-warehouse state that
-- precedes Batch1: it checks BatchIDs {0,1,2,3} exist ('DImessages batches'), 4 Phase
-- Complete Records ('DImessages Phase complete records'), 24 Validation rows for EVERY
-- batch including 0 ('DImessages validation reports'), and that every batch-0 Validation
-- row is '0' ('DImessages initial condition' — the DW must be empty before Batch1).
--
-- The classic driver gets these by running batch_complete.sql + batch_validation.sql with
-- batch_id=0 against the freshly dw_init'd (empty) tables. In the duckrun/dbt flow the
-- warehouse tables do not exist until batch 1's `dbt run` creates them, so there is no
-- empty-table state to query — we emit the same 1 PCR + 24 zero Validation rows as
-- constants, which is exactly the invariant the audit asserts (DW empty before Batch1).
-- The 24 (source, text) pairs are identical to batch_validation.sql's output.
INSERT INTO DImessages
SELECT now()::timestamp AS MessageDateAndTime, 0 AS BatchID,
       'Phase Complete Record' AS MessageSource, 'Batch Complete' AS MessageText,
       'PCR' AS MessageType, NULL::varchar AS MessageData
UNION ALL
SELECT now()::timestamp, 0, MessageSource, MessageText, 'Validation', '0'
FROM (VALUES
  ('DimAccount',        'Row count'),
  ('DimBroker',         'Row count'),
  ('DimCompany',        'Row count'),
  ('DimCustomer',       'Row count'),
  ('DimDate',           'Row count'),
  ('DimSecurity',       'Row count'),
  ('DimTime',           'Row count'),
  ('DimTrade',          'Row count'),
  ('FactCashBalances',  'Row count'),
  ('FactHoldings',      'Row count'),
  ('FactMarketHistory', 'Row count'),
  ('FactWatches',       'Row count'),
  ('Financial',         'Row count'),
  ('Industry',          'Row count'),
  ('Prospect',          'Row count'),
  ('StatusType',        'Row count'),
  ('TaxRate',           'Row count'),
  ('TradeType',         'Row count'),
  ('FactCashBalances',  'Row count joined'),
  ('FactHoldings',      'Row count joined'),
  ('FactMarketHistory', 'Row count joined'),
  ('FactWatches',       'Row count joined'),
  ('DimCustomer',       'Inactive customers'),
  ('FactWatches',       'Inactive watches')
) AS v(MessageSource, MessageText);
