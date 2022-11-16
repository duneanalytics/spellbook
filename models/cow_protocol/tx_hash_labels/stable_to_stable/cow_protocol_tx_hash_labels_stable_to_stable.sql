{{
    config(
        alias='stable_to_stable',
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "tx_hash_labels", \'["gentrexha"]\') }}'
    )
}}

with
 stable_to_stable_trades as (
    select
        distinct t.tx_hash
    where
        t.blockchain = 'ethereum'
        and token_pair in (
            'USDT-DAI',
            'USDT-USDC',
            'USDT-BUSD',
            'DAI-USDT',
            'DAI-USDC',
            'DAI-BUSD',
            'USDC-USDT',
            'USDC-DAI',
            'USDC-BUSD',
            'BUSD-USDC',
            'BUSD-USDT',
            'BUSD-DAI'
        )
  )
select
  array("ethereum") as blockchain,
  tx_hash,
  "Stable to stable" AS name,
  "stable_to_stable" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2022-11-16') as created_at,
  now() as updated_at
from
  stable_to_stable_trades