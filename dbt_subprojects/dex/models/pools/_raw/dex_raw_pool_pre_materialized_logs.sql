{{ config(
        schema='dex',
        alias = 'dex_raw_pool_pre_materialized_logs',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'blockchain', 'tx_hash', 'index']
    )
}}



{% for blockchain in dex_raw_pools_blockchains_macro() %}
    select 
        '{{ blockchain }}' as blockchain
        , topic0
        , topic1
        , topic2
        , topic3
        , data
        , block_number
        , block_time
        , contract_address
        , tx_hash
        , index
        , cast(date_trunc('month', block_time) as date) as block_month
    from {{ source(blockchain, 'logs') }}
    where topic0 in ({{ dex_raw_pools_logs_config_macro().keys() | join(',') }})
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}