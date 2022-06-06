 {{
  config(
        alias='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_id'
  )
}}

SELECT
    'solana' AS blockchain,
    t.block_time,
    p.symbol AS token_symbol,
    p.contract_address AS token_address,
    t.id AS trade_id,
    t.signatures[0] || t.id AS unique_id,
    t.signatures[0] AS tx_hash,
    abs(
        t.post_balances[0] / 1e9 - t.pre_balances[0] / 1e9
    ) * p.price AS amount_usd,
    abs(t.post_balances[0] / 1e9 - t.pre_balances[0] / 1e9) AS amount,
    t.account_keys[0] AS traders
FROM {{ source('solana','transactions') }} AS t
LEFT JOIN {{ source('prices', 'usd') }} AS p
          ON p.minute = date_trunc('minute', t.block_time)
          AND p.symbol = 'SOL'
WHERE
    (
        array_contains(
            t.account_keys, '3o9d13qUvEuuauhFrVom1vuCzgNsJifeaBYDPquaT73Y'
        )
        OR array_contains(
            t.account_keys, 'pAHAKoTJsAAe2ZcvTZUxoYzuygVAFAmbYmJYdWT886r'
        )
    )
    AND t.block_time > '2022-04-06'
    AND array_contains(t.log_messages, 'Program log: Instruction: ExecuteSale')
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    AND t.block_time > now() - INTERVAL 2 DAYS
{% endif %}
