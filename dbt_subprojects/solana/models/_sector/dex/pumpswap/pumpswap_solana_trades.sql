{{
  config(
        schema = 'pumpswap_solana',
        alias = 'trades',
        materialized = 'view',
        )
}}

-- Simple view that filters trades from the standardized dex_solana_trades table
select * from {{ref('dex_solana_trades')}}
where project = 'pumpswap'