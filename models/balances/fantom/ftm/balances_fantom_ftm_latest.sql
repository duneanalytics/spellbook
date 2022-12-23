{{ config(
        alias='ftm_latest',
        post_hook='{{ expose_spells_hide_trino(\'["fantom"]\',
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
    rh.amount*p.price as amount_usd,
    'FTM' as symbol, 
    rh.last_updated
FROM {{ ref('transfers_fantom_ftm_rolling_hour') }} rh
LEFT JOIN {{ source('prices', 'usd') }} p
    ON p.contract_address = rh.token_address
    AND p.minute = date_trunc('minute', rh.last_updated) - INTERVAL 10 minutes
    AND p.blockchain = 'fantom'
where rh.recency_index = 1
