{{
    config(
        alias = alias('tx_hash_labels_harvest_yield_ethereum'),
        tags=['dunesql']
    )
}}

with
  harvest_yield_tokens as (
    select
        harvest_yield_token_address
    from
        (
            VALUES  (0xba100000625a3754423978a60c9317c58a424e3d), -- BAL
                    (0xc0c293ce456ff0ed870add98a0828dd4d2903dbf), -- AURA
                    (0x6b3595068778dd592e39a122f4f5a5cf09c90fe2), -- SUSHI
                    (0x1f9840a85d5af5bf1d1762f925bdaddc4201f984), -- UNI
                    (0xd533a949740bb3306d119cc777fa900ba034cd52), -- CRV
                    (0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b), -- CVX
                    (0x6cacdb97e3fc8136805a9e7c342d866ab77d0957), -- SWAPR
                    (0x48c3399719b582dd63eb5aadf12a40b4c3f52fa2), -- SWISE
                    (0x875773784af8135ea0ef43b5a374aad105c5d39e), -- IDLE
                    (0xd33526068d116ce69f19a9ee46f0bd304f21a51f) -- RPL
        ) t(harvest_yield_token_address)
 ),

 harvest_yield_trades as (
    select
        *
    from (
        select tx_hash, evt_index, project, version
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        and token_sold_address in (select harvest_yield_token_address from harvest_yield_tokens)
        UNION ALL
        select tx_hash, evt_index, project, version
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
        and token_sold_address in (select harvest_yield_token_address from harvest_yield_tokens)
    )
 )

select
  'ethereum' as blockchain,
  concat(CAST(tx_hash AS VARCHAR), CAST(evt_index AS VARCHAR), project, version) as tx_hash_key,
  'Offramp to stable' AS name,
  'tx_hash' AS category,
  'gentrexha' AS contributor,
  'query' AS source,
  TIMESTAMP '2023-02-23' as created_at,
  now() as updated_at,
  'harvest_yield' as model_name,
  'usage' as label_type
from
  harvest_yield_trades
