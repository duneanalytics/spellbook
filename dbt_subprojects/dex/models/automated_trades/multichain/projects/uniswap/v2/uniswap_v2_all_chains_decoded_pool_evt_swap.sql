{{ config(
        schema = 'uniswap_v2_all_chains',
        alias = 'decoded_pool_evt_swap',
        materialized = 'view'
        )
}}

select * from {{ ref('uniswap_v2_old_chains_decoded_pool_evt_swap') }}
union all 
select * from {{ ref('uniswap_v2_new_chains_decoded_pool_evt_swap') }} 