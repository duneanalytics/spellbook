 {{
  config(
        schema = 'pumpdotfun_solana',
        alias = 'trades',
        materialized = 'view')
}}

select * from {{ref('dex_solana_trades')}}
where project = 'pumpdotfun'
