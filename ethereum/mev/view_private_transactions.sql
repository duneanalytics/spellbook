CREATE OR REPLACE VIEW mev.view_private_transactions AS
SELECT
    tx.block_time,
    tx.block_number,
    tx.hash,
    tx.from,
    tx.to,
    tx.gas_price,
    miner as miner_address,
    miner_bribe.value as bribe
FROM ethereum.transactions tx
INNER JOIN ethereum.blocks ON ethereum.blocks."number" = tx.block_number 
LEFT JOIN ethereum.traces miner_bribe on miner_bribe.tx_hash = tx."hash" 
          AND miner_bribe.success = True 
          AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
          AND miner_bribe."to" = miner
WHERE tx.gas_price = 0  AND tx.block_number < 12965000 -- pre eip 1559 method
