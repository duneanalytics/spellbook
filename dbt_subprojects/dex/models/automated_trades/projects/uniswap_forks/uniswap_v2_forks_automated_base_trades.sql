{{ config(
        schema = 'uniswap_v2_forks',
        alias = 'automated_base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_month', 'block_number', 'tx_index', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{%- set Pair_evt_Swap -%}
(
        select *
        from {{ ref('uniswap_v2_forks_old_chains_decoded_swap_events') }}
        {% if is_incremental() -%}
        WHERE {{ incremental_predicate('block_time') }}
        {%- endif %}
        union all 
        select *
        from {{ ref('uniswap_v2_forks_new_chains_decoded_swap_events') }}
        {% if is_incremental() -%}
        WHERE {{ incremental_predicate('block_time') }}
        {%- endif %}
)
{%- endset -%}

{%- set Factory_evt_PairCreated -%}
(
        select *
        from {{ ref('uniswap_v2_forks_old_chains_decoded_factory_events') }}
        union all 
        select *
        from {{ ref('uniswap_v2_forks_new_chains_decoded_factory_events') }}
)
{%- endset -%}

{{
    uniswap_v2_forks_trades(
        version = '2'
        , Pair_evt_Swap = Pair_evt_Swap
        , Factory_evt_PairCreated = Factory_evt_PairCreated
    )
}}