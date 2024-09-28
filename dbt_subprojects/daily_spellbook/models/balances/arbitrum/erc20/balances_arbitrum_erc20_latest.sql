{{ config(
        
        alias = 'erc20_latest',
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "balances",
                                    \'["Henrystats"]\') }}'
        )
}}

SELECT
    rh.wallet_address,
    rh.token_address,
    rh.amount_raw,
    rh.amount,
    rh.amount * p.price as amount_usd,
    rh.symbol,
    rh.last_updated
FROM 
{{ ref('transfers_arbitrum_erc20_rolling_hour') }} rh
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.contract_address = rh.token_address
    AND p.minute = date_trunc('minute', rh.last_updated) - Interval '10' Minute
    AND p.blockchain = 'arbitrum'
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN {{ ref('balances_arbitrum_erc20_noncompliant') }} nc
    ON rh.token_address = nc.token_address
WHERE rh.recency_index = 1
AND nc.token_address IS NULL