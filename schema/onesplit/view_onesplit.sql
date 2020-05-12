CREATE OR REPLACE MATERIALIZED VIEW onesplit.swaps AS SELECT * FROM (
    SELECT "fromToken" as from_token, "toToken" as to_token, "amount" as from_amount, "minReturn" as to_amount, "call_tx_hash" as tx_hash, "call_block_time" as block_time FROM onesplit."OneSplit_call_swap"     WHERE call_success UNION ALL
    SELECT "fromToken" as from_token, "toToken" as to_token, "amount" as from_amount, "minReturn" as to_amount, "call_tx_hash" as tx_hash, "call_block_time" as block_time FROM onesplit."OneSplit_call_goodSwap" WHERE call_success --UNION ALL
) tt;

CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_1 ON onesplit.swaps (from_token);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_2 ON onesplit.swaps (to_token);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_3 ON onesplit.swaps (from_amount);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_4 ON onesplit.swaps (to_amount);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_5 ON onesplit.swaps (tx_hash);
CREATE INDEX IF NOT EXISTS onesplit_swaps_idx_6 ON onesplit.swaps (block_time);

SELECT cron.schedule('0-59 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY onesplit.swaps');
