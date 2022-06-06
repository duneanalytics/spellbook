{{ config(
        alias='trades'
        )
}}

SELECT
    blockchain,
    project,
    version,
    tx_hash,
    block_time,
    amount_usd,
    amount,
    token_symbol,
    token_address,
    trade_id
FROM
    (
        SELECT
            blockchain,
            project,
            version,
            tx_hash,
            block_time,
            amount_usd,
            amount,
            token_symbol,
            token_address,
            trade_id
        FROM {{ ref('opensea_trades') }}
        UNION ALL
        SELECT
            blockchain,
            project,
            version,
            tx_hash,
            block_time,
            amount_usd,
            amount,
            token_symbol,
            token_address,
            trade_id
        FROM {{ ref('magiceden_trades') }})
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time > (SELECT max(block_time) FROM {{ this }})
{% endif %}
