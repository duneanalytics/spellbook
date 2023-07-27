{{
    config(
	tags=['legacy'],
	
        alias = alias('trader_dex_diversity', legacy_model=True),
        post_hook='{{ expose_spells(\'
        ["ethereum", "fantom", "arbitrum", "avalanche_c", "gnosis", "bnb", "optimism", "polygon"]\', 
        "sector", 
        "labels", 
        \'["gentrexha", "Henrystats"]\') }}'
    )
}}

with
 trader_dex_diversity as (
    select blockchain,
           count(distinct project) as dex_diversity,
           taker                   as address
    from (select blockchain, taker, project
          from {{ ref('dex_aggregator_trades_legacy') }}
          union all
          select blockchain, taker, project
          from {{ ref('dex_trades_legacy') }})
    group by taker, blockchain 
 )

select                                                      
    blockchain,
    address,
    concat('Number of DEXs traded on: ', dex_diversity) as name,
    "dex" AS category,
    "gentrexha" AS contributor,
    "query" AS source,
    timestamp('2022-12-15') as created_at,
    now() as updated_at,
    "trader_dex_diversity" as model_name,
    "usage" as label_type
from trader_dex_diversity