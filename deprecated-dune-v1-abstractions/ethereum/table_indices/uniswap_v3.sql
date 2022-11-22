-- uniswap_v3."Pair_call_swap"
CREATE INDEX CONCURRENTLY IF NOT EXISTS "uniswap_v3_Pair_call_swap_contract_address_idx" ON uniswap_v3."Pair_call_swap" (contract_address);
CREATE INDEX CONCURRENTLY IF NOT EXISTS "uniswap_v3_Pair_call_swap_call_block_time_idx" ON uniswap_v3."Pair_call_swap" (call_block_time);
CREATE INDEX CONCURRENTLY IF NOT EXISTS "uniswap_v3_Pair_call_swap_recipient_idx" ON uniswap_v3."Pair_call_swap" (recipient);
CREATE INDEX CONCURRENTLY IF NOT EXISTS "uniswap_v3_Pair_call_swap_call_success_idx" ON uniswap_v3."Pair_call_swap" (call_success);

-- uniswap_v3."Factory_call_createPool":
CREATE INDEX CONCURRENTLY IF NOT EXISTS "uniswap_v3_Factory_call_createPool_tokenA_idx" ON uniswap_v3."Factory_call_createPool" ("tokenA");
CREATE INDEX CONCURRENTLY IF NOT EXISTS "uniswap_v3_Factory_call_createPool_tokenB_idx" ON uniswap_v3."Factory_call_createPool" ("tokenB");
