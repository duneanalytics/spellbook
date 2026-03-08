{{
  config(
    schema = 'pumpswap_solana',
    alias = 'int_all_swaps',
    materialized = 'view'
  )
}}

SELECT
   *
FROM {{ ref('pumpswap_solana_stg_decoded_swaps')}}

UNION ALL 

SELECT 
*
FROM {{ref('pumpswap_solana_stg_decoded_newevent')}}
