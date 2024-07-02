 {{
  config(

        schema = 'lifinity_v1',
        alias = 'trades',
        materialized = 'view',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "lifinity",
                                    \'["ilemi"]\') }}')
}}
select * from {{ ref('dex_solana_trades' )}}
where project = 'lifinity' and version = 1
