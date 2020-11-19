CREATE OR REPLACE view stablecoin.view_transfers AS (

SELECT 
    "from",
    "to",
    (value/10^(decimals)) AS value,
    symbol,
    evt_block_time,
    name,
    evt_tx_hash,
    value AS value_raw
FROM erc20."ERC20_evt_Transfer" tr 
INNER JOIN erc20.stablecoins st ON tr.contract_address = st.contract_address
)
;
