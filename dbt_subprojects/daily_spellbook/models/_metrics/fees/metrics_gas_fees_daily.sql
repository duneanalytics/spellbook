{{ config(
        schema = 'metrics'
        , alias = 'gas_fees_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with daily_data as (
select
    blockchain
    , block_date
    , sum(tx_fee_usd) as gas_fees_usd
from
    {{ source('gas', 'fees') }}
{% if is_incremental() %}
where
    {{ incremental_predicate('block_date') }}
{% endif %}
group by
    blockchain
    , block_date
)

select
    blockchain
    ,block_date
    ,case when blockchain = 'tron' then gas_fees_usd * coalesce(trx_ration,0.0) -- apply correction to account for subsidized fees
    else gas_fees_usd end
    as gas_fees_usd
from daily_data
left join {{ref('tron_fee_correction')}}
on block_date = day and blockchain = 'tron'
