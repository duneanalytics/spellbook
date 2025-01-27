{{ config(
        schema = 'uniswap_v2_decoded_events',
        alias = 'all_chains_decoded_factory_evt',
        materialized = 'view'
        )
}}

select * from {{ ref('uniswap_v2_old_chains_decoded_factory_evt') }}
union all 
select * from {{ ref('uniswap_v2_new_chains_decoded_factory_evt') }} 