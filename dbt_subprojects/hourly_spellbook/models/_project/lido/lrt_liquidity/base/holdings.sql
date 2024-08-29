{{ config(
        schema='lido_lrt_liquidity_base',
        alias = 'holdings',
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "lido_lrt_liquidity",
                                \'["pipistrella"]\') }}'
        )
}}

SELECT  b.day,
        'base' as blockchain,
        b.address,
        h.namespace,
        h.category,
        h.paired_token,
        t.symbol,
        t.project,
        b.balance,
        b.balance_usd
FROM {{ source('tokens_base', 'balances_daily') }} b
JOIN {{ref('tokens')}} t on b.token_address = t.address and t.blockchain = 'base'
JOIN {{ref('holders')}} h on b.address = h.address and h.blockchain = 'base'  


  