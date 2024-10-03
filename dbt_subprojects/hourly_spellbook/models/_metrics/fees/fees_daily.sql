{{ config(
        schema = 'fees'
        , alias = 'daily'
        )
}}

select
    blockchain
    , block_date as day
    , sum(tx_fee_usd) as gas_spent_usd
from
    {{ ref('gas_fees') }}
group by
    blockchain
    , block_date