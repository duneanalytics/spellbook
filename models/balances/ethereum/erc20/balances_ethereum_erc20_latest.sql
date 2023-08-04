{{ config(
        alias = alias('erc20_latest'),
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke","dot2dotseurat"]\') }}'
        )
}}
SELECT
    rh.wallet_address,
    rh.token_address,
    rh.amount_raw,
    rh.amount,
    rh.amount*p.price as amount_usd,
    rh.symbol,
    rh.last_updated
FROM {{ ref('transfers_ethereum_erc20_rolling_hour') }} rh
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.contract_address = rh.token_address
    AND p.minute = date_trunc('minute', rh.last_updated) - INTERVAL 10 minutes
    AND p.blockchain = 'ethereum'
-- Removes rebase tokens from balances
LEFT JOIN {{ ref('tokens_ethereum_rebase') }}  as r
    ON rh.token_address = r.contract_address
-- Removes likely non-compliant tokens due to negative balances
LEFT JOIN {{ ref('balances_ethereum_erc20_noncompliant') }}  as nc
    ON rh.token_address = nc.token_address
where rh.recency_index = 1
and r.contract_address is null
and nc.token_address is null
