-- Ported from shannon-barrow/databricks-tpc-di
--   src/incremental_batches/audit_validation/batch_complete.sql
-- Writes the Phase Complete Record (PCR) to DImessages after each batch. The audit's
-- 'DImessages Phase complete records' check counts one PCR row per batch (3 total).
-- Dialect: ${...} widget substitution -> {{batch_id}} (run_sequential.py subs it);
--          CURRENT_TIMESTAMP() -> now() (connection TimeZone is set to UTC); bare
--          DImessages (schema is set on the duckrun connection).
INSERT INTO DImessages
SELECT
  now()::timestamp        AS MessageDateAndTime,
  {{batch_id}}            AS BatchID,
  'Phase Complete Record' AS MessageSource,
  'Batch Complete'        AS MessageText,
  'PCR'                   AS MessageType,
  NULL::varchar           AS MessageData;
