{{ config(
        schema = 'uniswap_v3_multichain',
        alias = 'all_chains_decoded_factory_evt',
        materialized = 'view'
        )
}}

select * from {{ ref('uniswap_v3_multichain_decoded_factory_evt') }}
union all
select * from {{ ref('uniswap_v3_new_chains_decoded_factory_evt') }} 