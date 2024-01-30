{{ config(
     alias = 'pool_incentives_rates'
    , materialized = 'table'
    )
}}

WITH last_block AS (
        SELECT MAX(block_number) AS max_bt
        FROM {{ source('optimism','transactions') }}
        WHERE block_time > NOW() - interval '7' day

)

, pools AS (
        SELECT
        evt_block_time, evt_block_number, evt_index,
        contract_address, rewarder AS rewarder_address, pid, allocPoint AS alloc_points
        FROM {{ source('sushi_optimism','MiniChefV2_evt_LogPoolAddition') }}

        UNION ALL

        SELECT
        evt_block_time, evt_block_number, evt_index,
        contract_address, rewarder AS rewarder_address, pid, allocPoint AS alloc_points
        FROM {{ source('sushi_optimism','MiniChefV2_evt_LogSetPool') }}
        WHERE overwrite

)

, rates_updates AS (
        SELECT contract_address,
               evt_block_time,
               evt_block_number,
               evt_index,
               -- how many tokens to emit per second (not decimal adjusted)
               sushiPerSecond                                                          AS tokens_per_second_raw,

               lead(evt_block_number, 1, (SELECT MAX_BT FROM LAST_BLOCK))
                    OVER (PARTITION BY contract_address ORDER BY evt_block_number ASC) AS next_evt_number
        FROM {{ source('sushi_optimism','MiniChefV2_evt_LogSushiPerSecond') }}
)

, pool_updates AS (
    SELECT p.evt_block_time,
           p.evt_block_number,
           p.evt_index,
           p.contract_address,
           rewarder_address,
           c.reward_token,
           p.pid,
           lp_address,
           alloc_points,
           lead(evt_block_number, 1, (SELECT MAX_BT FROM LAST_BLOCK))
                OVER (PARTITION BY p.contract_address, p.pid ORDER BY evt_block_number ASC) AS next_evt_number
    FROM pools p
    LEFT JOIN {{ ref('sushiswap_optimism_pool_incentives_config') }} c
        ON p.rewarder_address = c.contract_address
    LEFT JOIN {{ ref('sushiswap_optimism_pool_incentives_mappings') }} m
        ON p.contract_address = m.contract_address
        AND p.pid = m.pid
)

, events AS (
    SELECT evt_block_number, evt_block_time
    FROM (SELECT evt_block_number, evt_block_time
          FROM rates_updates
          UNION ALL
          SELECT evt_block_number, evt_block_time
          FROM pool_updates) a
    GROUP BY 1,2 -- get distinct event timestamps
)

, joined as (
        SELECT DATE_TRUNC('day', e.evt_block_time)                                                            AS block_date
             , e.evt_block_number
             , e.evt_block_time
             , COALESCE(pu.contract_address, ru.contract_address)                                             AS contract_address
             , pu.rewarder_address
             , pu.reward_token
             , pu.pid
             , pu.lp_address
             , pu.alloc_points
             , COALESCE(ru.tokens_per_second_raw, UINT256 '0')                                                          AS tokens_per_second_raw
             , SUM(pu.alloc_points)
                   OVER (PARTITION BY e.evt_block_number, COALESCE(pu.contract_address, ru.contract_address)) AS total_alloc_points
        FROM events e
        FULL OUTER JOIN pool_updates pu
            ON e.evt_block_number >= pu.evt_block_number
            AND e.evt_block_number < pu.next_evt_number
        FULL OUTER JOIN rates_updates ru
            ON e.evt_block_number >= ru.evt_block_number
            AND e.evt_block_number < ru.next_evt_number
            AND pu.contract_address = ru.contract_address
)


SELECT block_date
     , 'optimism'                                                                                      AS blockchain
     , evt_block_time
     , evt_block_number
     , a.contract_address
     , rewarder_address
     , reward_token
     , pid
     , lp_address
     , alloc_points
     , total_alloc_points
     , alloc_point_share
     , tokens_per_second_raw
     , tokens_per_second_raw * alloc_point_share                                                       AS alloc_tokens_per_second_raw
     , cast(tokens_per_second_raw * alloc_point_share as double) /
       cast(power(10, decimals) as double)                                                             AS alloc_tokens_per_second
     , symbol                                                                                          AS reward_token_symbol
     , decimals                                                                                        AS reward_token_decimals
FROM (
        SELECT *
        , cast(alloc_points as double) / cast(total_alloc_points as double) AS alloc_point_share
        FROM joined
) a
LEFT JOIN {{ source('tokens_optimism', 'erc20') }} t
        ON a.reward_token = t.contract_address
        
