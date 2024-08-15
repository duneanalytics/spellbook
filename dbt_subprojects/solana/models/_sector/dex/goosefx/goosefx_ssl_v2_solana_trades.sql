 {{
  config(
        schema = 'goosefx_ssl_v2_solana',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "goosefx",
                                    \'["ilemi"]\') }}')
}}
select
      *
from
      {{ ref('dex_solana_trades') }}
where 
      project = 'goosefx_ssl'
      and version = 2