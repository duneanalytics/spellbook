CREATE MATERIALIZED VIEW onesplit.view_swaps AS
SELECT * FROM (
    SELECT "fromToken" as from_token, "toToken" as to_token, "amount" as from_amount, "minReturn" as to_amount, call_tx_hash as tx_hash, call_trace_address, call_block_time as block_time FROM onesplit."OneSplit_call_swap"     WHERE call_success UNION ALL
    SELECT "fromToken" as from_token, "toToken" as to_token, "amount" as from_amount, "minReturn" as to_amount, call_tx_hash as tx_hash, call_trace_address, call_block_time as block_time FROM onesplit."OneSplit_call_goodSwap" WHERE call_success
) tt;

CREATE UNIQUE INDEX IF NOT EXISTS onesplit_swaps_unique_idx ON onesplit.view_swaps (tx_hash, call_trace_address);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_1 ON onesplit.view_swaps (from_token) INCLUDE (from_amount);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_2 ON onesplit.view_swaps (to_token) INCLUDE (to_amount);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_3 ON onesplit.view_swaps (block_time);

SELECT cron.schedule('0-59 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY onesplit.view_swaps');
