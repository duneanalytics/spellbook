{{ config(
        schema='prices',
        alias ='stability_rank',
        post_hook='{{ expose_spells(\'["ethereum", "solana", "arbitrum", "gnosis", "optimism", "bnb", "avalanche_c", "polygon", "fantom"]\',
                                    "sector",
                                    "prices",
                                    \'["hosuke"]\') }}'
        )
}}

WITH recent_dex_trades AS (
    SELECT *
    FROM {{ ref('dex_trades') }}
    WHERE block_date >= now() - interval '7 day'
)
, prices_usd_stability AS (
    SELECT blockchain
         , contract_address
         -- amplifying the effect of the stability metric, odd powers are used to preserve the sign
         , power(1 - stddev(price) / avg(price), 7) AS stability
         , avg(price) AS avg_price
         , stddev(price) AS stddev_price
    FROM {{ source('prices', 'usd') }} p
    WHERE p.minute >= now() - interval '7 day'
    GROUP BY blockchain, contract_address
)
, prices_usd_stability_rank AS (
    SELECT p.blockchain
         , p.contract_address
         , erc20.symbol
         , stability
         , avg_price
         , stddev_price
         , rank() OVER (ORDER BY stability DESC) AS stat_stability_rank
    FROM prices_usd_stability p
    LEFT JOIN {{ ref('tokens_erc20') }} erc20
        ON erc20.contract_address = p.contract_address
        AND erc20.blockchain = p.blockchain
)
, token_occurances AS (
    SELECT blockchain
         , contract_address
         , SUM(occurances) AS total_occurances
    FROM (
        SELECT blockchain
             , token_bought_address AS contract_address
             , count(*) AS occurances
        FROM recent_dex_trades
        GROUP BY blockchain, token_bought_address

        UNION ALL

        SELECT blockchain
             , token_sold_address AS contract_address
             , count(*) AS occurances
        FROM recent_dex_trades
        GROUP BY blockchain, token_sold_address
    ) t
    GROUP BY blockchain, contract_address
)
, max_token_occurences AS (
    SELECT blockchain
         , max(total_occurances) AS max_occurances
    FROM token_occurances
    GROUP BY blockchain
)
, token_occurances_rank AS (
    SELECT blockchain
         , contract_address
         , total_occurances
         , rank() OVER (PARTITION BY blockchain ORDER BY total_occurances DESC) AS occurances_rank
    FROM token_occurances
)
, weighted_prices_usd_stability AS (
    SELECT p.blockchain
         , p.contract_address
         , p.symbol
         , p.stability
         , p.avg_price
         , p.stddev_price
         , p.stat_stability_rank
         , o.total_occurances
         , o.occurances_rank
         -- nomalizing frequency of occurance
         , p.stability * sqrt(sqrt(o.total_occurances / m.max_occurances)) AS weighted_stability
    FROM prices_usd_stability_rank p
    LEFT JOIN token_occurances_rank o
        ON o.blockchain = p.blockchain
        AND o.contract_address = p.contract_address
    LEFT JOIN max_token_occurences m
        ON m.blockchain = p.blockchain
)
, weighted_prices_usd_stability_rank AS (
    SELECT blockchain
         , contract_address
         , symbol
         , rank() OVER (PARTITION BY blockchain ORDER BY weighted_stability DESC) AS chain_stab_rank
         , weighted_stability
         , stability
         , avg_price
         , stddev_price
         , stat_stability_rank
         , total_occurances
         , occurances_rank
         , rank() OVER (ORDER BY weighted_stability DESC) AS stability_rank
    FROM weighted_prices_usd_stability
)
SELECT *
FROM weighted_prices_usd_stability_rank
ORDER BY stability_rank ASC
;