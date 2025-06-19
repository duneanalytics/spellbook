{{
    config(
        schema = 'ton',
        alias='prices_daily',
        
        materialized = 'table',
        unique_key = ['blockchain', 'token_address', 'timestamp'],
        post_hook='{{ expose_spells(\'["ton"]\',
                                    "sector",
                                    "ton",
                                    \'["pshuvalov", "okhlopkov"]\') }}'
    )
}}



WITH
ALL_TRADES AS (
    SELECT 
        token_address,
        amount_raw,
        volume_usd,
        volume_ton,
        block_date,
        block_time,
        trader_address
    FROM {{ source('ton', 'dex_trades') }}
    CROSS JOIN UNNEST (ARRAY[
      ROW(token_bought_address, amount_bought_raw), 
      ROW(token_sold_address, amount_sold_raw)
      ]) AS T(token_address, amount_raw)
)


, LIQUID_TOKENS AS (
    SELECT DISTINCT 
        block_date,
        token_address
    FROM {{ source('ton', 'dex_pools') }} DP
    CROSS JOIN UNNEST (ARRAY[
        ROW(jetton_left, reserves_left), 
        ROW(jetton_right, reserves_right)
        ]) AS T(token_address, reserves_raw)
    WHERE 1=1
        AND tvl_usd >= 100_000 -- highly liquid pools
)



, PRICES_FROM_DEX_TRADES AS (
    SELECT
        T.token_address,
        DATE_TRUNC('day', T.block_time) AS ts,
        CASE
          WHEN T.token_address = '0:0000000000000000000000000000000000000000000000000000000000000000' THEN 1e-9
          ELSE SUM(T.volume_ton) / SUM(CAST(T.amount_raw AS DOUBLE))
        END AS price_ton,
        
        CASE
          WHEN T.token_address = '0:B113A994B5024A16719F69139328EB759596C38A25F59028B146FECDC3621DFE' THEN 1e-6
          ELSE SUM(T.volume_usd) / SUM(CAST(T.amount_raw AS DOUBLE))
        END AS price_usd,
        'Jetton' AS asset_type
    FROM ALL_TRADES AS T
    LEFT JOIN LIQUID_TOKENS
        ON LIQUID_TOKENS.token_address = T.token_address
        AND LIQUID_TOKENS.block_date = T.block_date
    GROUP BY 1, 2
    HAVING 1=1 -- threshold for inclusion token into the price table
        AND COUNT(*) >= 100  -- 100 trades / day
        AND SUM(T.volume_usd) >= 100  -- 100$ volume per day
        AND COUNT(DISTINCT T.trader_address) >= 10 -- 10 traders / day
        OR COUNT(LIQUID_TOKENS.token_address) > 0
)

------------------------------------------
----------DEX LP TOKENS-------------------
------------------------------------------

, PRICES_LP_TOKENS AS (
    SELECT 
        pool AS token_address, 
        block_date AS ts,
        MAX_BY(CAST(tvl_ton AS DOUBLE) / total_supply, block_time) AS price_ton,
        MAX_BY(CAST(tvl_usd AS DOUBLE) / total_supply, block_time) AS price_usd,
        'DEX LP' AS asset_type
    FROM {{ source('ton', 'dex_pools') }}
    WHERE 1=1
        AND total_supply > 0
        AND tvl_usd > 1000  -- $1000 TVL 
    GROUP BY 1,2
    HAVING 1=1
        -- AND COUNT(*) >= 2 -- 2 LP CHANGES/day, maybe too much
)


----------------------------------------------
--------------SLP TOKENS----------------------
-- https://docs.storm.tg/platform/liquidity --
----------------------------------------------


, SLPs as ( -- list of slp jettons and underlying assets
  SELECT 'NOT-SLP' as asset,
  UPPER('0:2ab634cfcbdbe3b97503691e0780c3d07c9069210a2b24b991ba4f9941b453f9') as slp_address,
  UPPER('0:2f956143c461769579baef2e32cc2d7bc18283f40d20bb03e432cd603ac33ffc') as underlying_asset

  UNION ALL

  SELECT 'USDT-SLP' as asset,
  UPPER('0:aea78c710ae94270dc263a870cf47b4360f53cc5ed38e3db502e9e9afb904b11') as slp_address,
  UPPER('0:b113a994b5024a16719f69139328eb759596c38a25f59028b146fecdc3621dfe') as underlying_asset

  UNION ALL

  SELECT 'TON-SLP' as asset,
  UPPER('0:8d636010dd90d8c0902ac7f9f397d8bd5e177f131ee2cca24ce894f15d19ceea') as slp_address,
  UPPER('0:0000000000000000000000000000000000000000000000000000000000000000') as underlying_asset
  
)

, SLP_MINTS AS ( -- get all mints event
  SELECT trace_id, je.amount AS slp_amount, block_date, SLPs.* 
  FROM {{ source('ton', 'jetton_events') }} je
  JOIN SLPs 
      ON je.jetton_master = SLPs.slp_address
  WHERE 1=1
      AND type = 'mint'
      AND NOT tx_aborted
)

, SLP_DEPOSITS AS ( -- each mint has deposit

    -- jettons assets
    SELECT SLP_MINTS.*, je.amount as underlying_asset_amount 
    FROM {{ source('ton', 'jetton_events') }} je
    JOIN SLP_MINTS ON 1=1
        AND SLP_MINTS.block_date = je.block_date 
        AND je.trace_id = SLP_MINTS.trace_id
        AND je.jetton_master = SLP_MINTS.underlying_asset 
        AND NOT tx_aborted 
        AND SLP_MINTS.underlying_asset != '0:0000000000000000000000000000000000000000000000000000000000000000'
    
    -- special version for native TON 
    -- let's take the first message (contains TON to be deposited + ~0.4 for gas fees) 
    -- minus the last one (contains excesses)

    UNION ALL
    
    SELECT 
        SLP_MINTS.*,
        MIN_BY(value, created_lt) - MAX_BY(value, created_lt) AS underlying_asset_amount 
    FROM {{ source('ton', 'messages') }} M
    JOIN SLP_MINTS ON 1=1
        AND SLP_MINTS.block_date = M.block_date 
        AND M.trace_id = SLP_MINTS.trace_id
        AND SLP_MINTS.underlying_asset = '0:0000000000000000000000000000000000000000000000000000000000000000'
    GROUP BY 1, 2, 3, 4, 5, 6

)

, SLP_ADDRESS_INTER_PRICE AS (
    SELECT 
        block_date, asset, slp_address, underlying_asset, 
        CAST(1.0 AS DOUBLE) * SUM(underlying_asset_amount) / SUM(slp_amount) AS price
    FROM SLP_DEPOSITS
    GROUP BY 1, 2, 3, 4
)

, PRICES_SLP AS (
    SELECT 
        slp_address AS token_address,
        block_date AS ts,
        SLP_ADDRESS_INTER_PRICE.price * P.price_ton AS price_ton,
        SLP_ADDRESS_INTER_PRICE.price * P.price_usd AS price_usd,
        'SLP' AS asset_type
    FROM SLP_ADDRESS_INTER_PRICE
    INNER JOIN PRICES_FROM_DEX_TRADES P
        ON P.token_address = SLP_ADDRESS_INTER_PRICE.underlying_asset
        AND P.ts = SLP_ADDRESS_INTER_PRICE.block_date
)

------------------------------------------
-----------FINAL GRAND MERGE -------------
------------------------------------------

SELECT 
    'ton' AS blockchain,
    COALESCE(
        PRICES_SLP.token_address,
        PRICES_LP_TOKENS.token_address,
        PRICES_FROM_DEX_TRADES.token_address
    ) AS token_address,
    
    COALESCE(
        PRICES_SLP.ts,
        PRICES_LP_TOKENS.ts,
        PRICES_FROM_DEX_TRADES.ts
    ) AS timestamp,

    COALESCE(
        PRICES_SLP.price_ton,
        PRICES_LP_TOKENS.price_ton,
        PRICES_FROM_DEX_TRADES.price_ton
    ) AS price_ton,

    COALESCE(
        PRICES_SLP.price_usd,
        PRICES_LP_TOKENS.price_usd,
        PRICES_FROM_DEX_TRADES.price_usd
    ) AS price_usd,

COALESCE(
        PRICES_SLP.asset_type,
        PRICES_LP_TOKENS.asset_type,
        PRICES_FROM_DEX_TRADES.asset_type
    ) AS asset_type
    
FROM PRICES_FROM_DEX_TRADES
FULL OUTER JOIN PRICES_SLP
    ON PRICES_FROM_DEX_TRADES.token_address = PRICES_SLP.token_address
    AND PRICES_FROM_DEX_TRADES.ts = PRICES_SLP.ts
FULL OUTER JOIN PRICES_LP_TOKENS
    ON PRICES_FROM_DEX_TRADES.token_address = PRICES_LP_TOKENS.token_address
    AND PRICES_FROM_DEX_TRADES.ts = PRICES_LP_TOKENS.ts