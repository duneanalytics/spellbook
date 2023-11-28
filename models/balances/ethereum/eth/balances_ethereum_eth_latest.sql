{{ config(
         schema = 'balances_ethereum',
        alias = 'ethereum_latest',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "balances",
                                    \'["rantum"]\') }}'
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
{{ ref('transfers_ethereum_eth_rolling_hour') }} rh
LEFT JOIN 
{{ source('prices', 'usd') }} p
    ON p.contract_address = rh.token_address
    AND p.minute = date_trunc('minute', rh.last_updated) - Interval '10' Minute
    AND p.blockchain = 'ethereum'
WHERE rh.recency_index = 1
