{{
  config(
    schema = 'gmx_v2_avalanche_c',
    alias = 'erc20',    
    materialized = 'view'
    )
}}

{%- set blockchain_name = 'avalanche_c' -%}

SELECT
    blockchain
    , project
    , symbol
    , contract_address
    , decimals
    , synthetic
    , last_update_utc
FROM {{ source('gmx-io', 'result_gmx_v_2_erc_20_from_api', database='dune') }}
WHERE blockchain = '{{ blockchain_name }}'