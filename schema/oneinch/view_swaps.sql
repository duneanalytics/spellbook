BEGIN;
DROP MATERIALIZED VIEW IF EXISTS oneinch.view_swaps;
CREATE MATERIALIZED VIEW oneinch.view_swaps AS SELECT * FROM (
    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash,  call_block_time as block_time, call_trace_address     FROM oneinch."exchange_v1_call_aggregate" WHERE call_success UNION ALL
    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address      FROM oneinch."exchange_v2_call_aggregate" WHERE call_success UNION ALL
    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address      FROM oneinch."exchange_v3_call_aggregate" WHERE call_success UNION ALL
    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address      FROM oneinch."exchange_v4_call_aggregate" WHERE call_success UNION ALL
    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address      FROM oneinch."exchange_v5_call_aggregate" WHERE call_success UNION ALL
    SELECT "fromToken" as from_token, "toToken" as to_token, "tokensAmount" as from_amount, "minTokensAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address      FROM oneinch."exchange_v6_call_aggregate" WHERE call_success UNION ALL
    SELECT "fromToken" as from_token, "toToken" as to_token, "fromTokenAmount" as from_amount, "minReturnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address   FROM oneinch."exchange_v7_call_swap"      WHERE call_success UNION ALL
    SELECT "fromToken" as from_token, "toToken" as to_token, "fromTokenAmount" as from_amount, "minReturnAmount" as to_amount, call_tx_hash as tx_hash, call_block_time as block_time, call_trace_address   FROM oneinch."OneInchExchange_call_swap"  WHERE call_success -- UNION ALL
) tt;

CREATE UNIQUE INDEX oneinch_swaps_unique_idx ON oneinch.view_swaps (tx_hash, call_trace_address);
CREATE INDEX IF NOT EXISTS oneinch_swaps_idx_1 ON oneinch.view_swaps (from_token) INCLUDE (from_amount);
CREATE INDEX IF NOT EXISTS oneinch_swaps_idx_2 ON oneinch.view_swaps (to_token) INCLUDE (to_amount);
CREATE INDEX IF NOT EXISTS oneinch_swaps_idx_3 ON oneinch.view_swaps (block_time);

SELECT cron.schedule('0-59 * * * *', 'REFRESH MATERIALIZED VIEW CONCURRENTLY oneinch.view_swaps');
COMMIT;
