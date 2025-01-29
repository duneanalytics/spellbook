CREATE OR REPLACE VIEW gmx_arbitrum.liquidations AS
WITH liquidation_events AS (
    SELECT
        evt_block_time,
        evt_tx_hash,
        account as liquidated_account,
        collateralToken,
        indexToken,
        isLong,
        size/1e30 as position_size_usd,
        collateral/1e30 as collateral_usd,
        reserveAmount,
        contract_address
    FROM
        {{ source('gmx_arbitrum', 'Vault_evt_LiquidatePosition') }}
),
token_prices AS (
    SELECT
        minute,
        contract_address,
        price
    FROM
        {{ source('prices', 'usd') }}
    WHERE
        blockchain = 'arbitrum'
)

SELECT
    le.evt_block_time,
    le.evt_tx_hash,
    le.liquidated_account,
    le.collateralToken,
    le.indexToken,
    le.isLong,
    le.position_size_usd,
    le.collateral_usd,
    le.position_size_usd * tp.price as liquidation_value_usd,
    le.collateral_usd * tp.price as collateral_value_usd,
    tp.price as token_price
FROM
    liquidation_events le
LEFT JOIN
    token_prices tp
    ON le.indexToken = tp.contract_address
    AND date_trunc('minute', le.evt_block_time) = tp.minute
; 
