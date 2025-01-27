{{ config(
        schema = 'uniswap_v3_decoded_events',
        alias = 'all_chains_decoded_pool_evt_swap',
        materialized = 'view'
        )
}}

select * from {{ ref('uniswap_v3_old_chains_decoded_pool_evt_swap') }}
union all 
select * from {{ ref('uniswap_v3_new_chains_decoded_pool_evt_swap') }} 