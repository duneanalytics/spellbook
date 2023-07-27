{{
    config(
	tags=['legacy'],
	
        alias = alias('tx_hash_labels_bluechip_investment_ethereum', legacy_model=True),
    )
}}

with
 bluechips as (
    select
        bluechip_address
    from
        (
            VALUES  ('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'), -- wBTC
                    ('0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'), -- WETH
                    ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee')  -- ETH
        ) t(bluechip_address)
 ),

 bluechip_investment_trades as (
    select
        *
    from (
        select tx_hash, evt_index, project, version
        from {{ ref('dex_aggregator_trades_legacy') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select bluechip_address from bluechips)
        and token_sold_address not in (select bluechip_address from bluechips)
        UNION ALL
        select tx_hash, evt_index, project, version
        from {{ ref('dex_trades_legacy') }}
        where blockchain = 'ethereum'
        and token_bought_address in (select bluechip_address from bluechips)
        and token_sold_address not in (select bluechip_address from bluechips)
    )
 )

select
  "ethereum" as blockchain,
  concat(tx_hash, CAST(evt_index AS VARCHAR(100)), project, version) as tx_hash_key,
  "Bluechip Investment" as name,
  "tx_hash" as category,
  "gentrexha" as contributor,
  "query" as source,
  CAST('2023-02-21' AS TIMESTAMP) as created_at,
  now() as updated_at,
  "bluechip_investment" as model_name,
  "usage" as label_type
from
  bluechip_investment_trades
