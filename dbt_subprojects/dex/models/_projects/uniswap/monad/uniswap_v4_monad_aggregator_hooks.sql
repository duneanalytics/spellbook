{{ config(
    schema = 'uniswap_v4_monad'
    , alias = 'aggregator_hooks'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'address']
    )
}}

-- Per-chain registry of Uniswap V4 BaseAggregatorHook contracts.
-- Same detection as uniswap_v4.aggregator_hooks, scoped to this chain so a new
-- V4 chain can be onboarded without marking every other chain's swaps modified.

{{ uniswap_v4_chain_aggregator_hooks('monad') }}
