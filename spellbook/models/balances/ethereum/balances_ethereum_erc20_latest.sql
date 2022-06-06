{{ config(
        alias='erc20_latest'
        )
}}
SELECT
    rh.wallet_address,
    rh.token_address,
    rh.amount_raw,
    rh.amount,
    rh.symbol,
    rh.last_updated,
    rh.amount * p.price AS amount_usd
FROM {{ ref('transfers_ethereum_erc20_rolling_hour') }} AS rh
LEFT JOIN {{ source('prices', 'usd') }} AS p
    ON p.contract_address = rh.token_address
        AND p.minute = date_trunc(
            'minute', rh.last_updated
        ) - INTERVAL 10 MINUTES
        AND p.blockchain = 'ethereum'
-- Removes rebase tokens from balances
LEFT JOIN {{ ref('tokens_ethereum_rebase') }} AS r
    ON rh.token_address = r.contract_address
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN {{ ref('balances_ethereum_erc20_noncompliant') }} AS nc
    ON rh.token_address = nc.token_address
WHERE rh.recency_index = 1
    AND p.minute IS NOT NULL
    AND r.contract_address IS NULL
    AND nc.token_address IS NULL
