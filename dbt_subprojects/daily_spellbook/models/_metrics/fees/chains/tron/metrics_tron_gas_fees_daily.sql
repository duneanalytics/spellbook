{{ config(
        schema = 'metrics_tron'
        , alias = 'gas_fees_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with fees as (
    select
        blockchain
        , block_date
        , sum(tx_fee_usd) as gas_fees_usd
    from
        {{ source('gas', 'fees') }}
    where blockchain = 'tron'
    {% if is_incremental() %}
    and
        {{ incremental_predicate('block_date') }}
    {% endif %}
    group by
        blockchain
        , block_date
)

select
    blockchain
    ,block_date
    ,gas_fees_usd * coalesce(t.trx_fee_ratio,0.0) as gas_fees_usd -- apply correction to account for subsidized fees
from fees
left join {{ref('tron_fee_correction')}} t
    on block_date = t.day
    and blockchain = 'tron'
    {% if is_incremental() %}
    and {{ incremental_predicate('day') }}
    {% endif %}
