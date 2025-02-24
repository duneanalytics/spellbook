{{ config(
        schema = 'uniswap_v3_decoded_events',
        alias = 'all_chains_automated_base_trades',
        partition_by = ['block_month', 'blockchain'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_number', 'tx_index', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

{%- set Pair_evt_Swap -%}
(
    select * from {{ ref('uniswap_v3_old_chains_decoded_pool_evt_swap') }}
    union all 
    select * from {{ ref('uniswap_v3_new_chains_decoded_pool_evt_swap') }}
)
{%- endset -%}

{%- set Factory_evt_PoolCreated -%}
(
    select * from {{ ref('uniswap_v3_old_chains_decoded_factory_evt') }}
    union all 
    select * from {{ ref('uniswap_v3_new_chains_decoded_factory_evt') }}
)
{%- endset -%}

{{
    uniswap_v3_forks_trades(
        version = '3'
        , Pair_evt_Swap = Pair_evt_Swap
        , Factory_evt_PoolCreated = Factory_evt_PoolCreated
    )
}}