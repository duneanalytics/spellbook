{{ config(
        schema = 'uniswap_v3_multichain',
        alias = 'all_chains_decoded_pool_evt_swap',
        materialized = 'view'
        )
}}

select * from {{ ref('uniswap_v3_multichain_decoded_pool_evt_swap') }}
union all
select * from {{ ref('uniswap_v3_new_chains_decoded_pool_evt_swap') }} 