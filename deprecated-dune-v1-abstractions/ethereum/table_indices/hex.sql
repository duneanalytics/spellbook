-- hex."HEX_evt_StakeEnd"
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_StakeEnd_stakeId_idx" ON hex."HEX_evt_StakeEnd" ("stakeId");
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_StakeEnd_stakerAddr_idx" ON hex."HEX_evt_StakeEnd" ("stakerAddr");
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_StakeEnd_evt_block_time_idx" ON hex."HEX_evt_StakeEnd" (evt_block_time);
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_StakeEnd_evt_block_number_idx" ON hex."HEX_evt_StakeEnd" (evt_block_number);

-- hex."HEX_evt_StakeStart"
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_StakeStart_stakeId_idx" ON hex."HEX_evt_StakeStart" ("stakeId");
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_StakeStart_stakerAddr_idx" ON hex."HEX_evt_StakeStart" ("stakerAddr");
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_StakeStart_evt_block_time_idx" ON hex."HEX_evt_StakeStart" (evt_block_time);
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_StakeStart_evt_block_number_idx" ON hex."HEX_evt_StakeStart" (evt_block_number);

-- hex."HEX_evt_Transfer"
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_Transfer_to_idx" ON hex."HEX_evt_Transfer" ("to");
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_Transfer_from_idx" ON hex."HEX_evt_Transfer" ("from");
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_Transfer_evt_block_time_idx" ON hex."HEX_evt_Transfer" (evt_block_time);
CREATE INDEX CONCURRENTLY IF NOT EXISTS "hex_HEX_evt_Transfer_evt_block_number_idx" ON hex."HEX_evt_Transfer" (evt_block_number);
