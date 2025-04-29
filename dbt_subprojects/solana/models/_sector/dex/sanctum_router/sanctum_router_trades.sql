 {{
  config(
        schema = 'sanctum_router',
        alias = 'trades',
        post_hook='{{ expose_spells(\'["solana"]\',
                                    "project",
                                    "sanctum_router",
                                    \'["senyor-kodi"]\') }}'
      )
}}

-- backwards compatible view so we don't break any user queries
select * from {{ref('dex_solana_trades')}}
where project = 'sanctum_router' and version = 1