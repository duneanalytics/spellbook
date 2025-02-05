 {{
  config(
        schema = 'goosefx_gamma_solana',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "goosefx",
                                    \'["ilemi"]\') }}')
}}
select * from {{ ref('dex_solana_trades') }}
where project = 'goosefx_ssl' 