{{ config(
    schema = 'slugs_optimism',
    alias = 'rewards',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['project','tx_hash','sub_tx_id'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    )
}}

select
    'optimism' as blockchain
    ,'slugs' as project
    ,'v1' as version
    ,evt_block_number as block_number
    ,evt_block_time as block_time
    ,cast(date_trunc('day',evt_block_time) as date) as block_date
    ,cast(date_trunc('month',evt_block_time) as date) as block_month
    ,evt_tx_hash as tx_hash
    ,'NFT' as category
    ,referrer as referrer_address
    ,sender as referee_address
    ,{{ var("ETH_ERC20_ADDRESS") }} as currency_contract
    ,case
            when length(url_encode(slug)) = 1 then 1 / 2.0e0
            when length(url_encode(slug)) = 2 then 0.5 / 2.0e0
            when length(url_encode(slug)) = 3 then 0.25 / 2.0e0
            when length(url_encode(slug)) = 4 then 0.1 / 2.0e0
            when length(url_encode(slug)) = 5 then 0.05 / 2.0e0
            when length(url_encode(slug)) = 6 then 0.03 / 2.0e0
            when length(url_encode(slug)) = 7 then 0.02 / 2.0e0
            else 0.01 / 2.0e0
        end as reward_amount_raw
    ,contract_address as project_contract_address
    ,evt_index as sub_tx_id
    ,tx."from" as tx_from
    ,tx."to" as tx_to
from {{ source('slugs_optimism', 'Slugs_evt_NewSlug') }} e
inner join {{ source('optimism', 'transactions') }} tx
    on evt_block_number = tx.block_number
    and evt_tx_hash = tx.hash
    {% if is_incremental() %}
    and {{incremental_predicate('tx.block_time')}}
    {% endif %}
where "isCustom" = true
{% if is_incremental() %}
and {{ incremental_predicate('evt_block_time') }}
{% endif %}
