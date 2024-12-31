 {{
  config(
        schema = 'dflow_clearpool_solana',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "dflow",
                                    \'["smyyguy"]\') }}')
}}
select
      *
from
      {{ ref('dex_solana_trades') }}
where 
      project = 'dflow'
      and version = 1
