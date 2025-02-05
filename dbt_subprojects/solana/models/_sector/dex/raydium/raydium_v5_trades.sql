 {{
  config(
        schema = 'raydium_v5',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "raydium",
                                    \'["0xsharples"]\') }}')
}}

select * from {{ref('dex_solana_trades')}}
where project = 'raydium' and version = 5
