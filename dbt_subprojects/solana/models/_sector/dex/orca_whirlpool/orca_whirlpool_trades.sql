 {{
  config(

        schema = 'orca_whirlpool',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "orca_whirlpool",
                                    \'["ilemi"]\') }}')
}}
select * from {{ref('dex_solana_trades')}}
where project = 'whirlpool'
