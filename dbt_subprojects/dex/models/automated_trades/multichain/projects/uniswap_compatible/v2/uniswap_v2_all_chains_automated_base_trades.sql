{{ config(
        schema = 'uniswap_v2_decoded_events',
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
    select * from {{ ref('uniswap_v2_old_chains_decoded_pool_evt_swap') }}
    union all 
    select * from {{ ref('uniswap_v2_new_chains_decoded_pool_evt_swap') }}
)
{%- endset -%}

{%- set Factory_evt_PairCreated -%}
(
    select * from {{ ref('uniswap_v2_old_chains_decoded_factory_evt') }}
    union all 
    select * from {{ ref('uniswap_v2_new_chains_decoded_factory_evt') }}
)
{%- endset -%}

{{
    uniswap_v2_forks_trades(
        version = '2'
        , Pair_evt_Swap = Pair_evt_Swap
        , Factory_evt_PairCreated = Factory_evt_PairCreated
    )
}}