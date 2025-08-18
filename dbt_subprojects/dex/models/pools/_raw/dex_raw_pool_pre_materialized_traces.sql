{{ config(
        schema='dex',
        alias = 'dex_raw_pool_pre_materialized_traces',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['block_month', 'blockchain', 'tx_hash', 'trace_address']
    )
}}



{% for blockchain in dex_raw_pools_blockchains_macro() %}
    select 
        '{{ blockchain }}' as blockchain
        , input
        , output
        , block_number
        , block_time
        , "to"
        , tx_hash
        , trace_address
        , cast(date_trunc('month', block_time) as date) as block_month
    from {{ source(blockchain, 'traces') }}
    where substr(input, 1, 4) in (
            {{ dex_raw_pools_traces_config_macro().keys() | join(',') }}
            , 0x485cc955 -- uniswap v2 pool initialization for further exclusion
        )
        -- and length(output) = 32
        and success
        and tx_success
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% endif %}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}