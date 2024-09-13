 {{
  config(
        schema = 'phoenix_v1',
        alias = 'trades',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "phoenix",
                                    \'["ilemi","jarryx"]\') }}'
      )
}}

-- backwards compatible view so we don't break any user queries
select * from {{ref('dex_solana_trades')}}
where project = 'phoenix' and version = 1
