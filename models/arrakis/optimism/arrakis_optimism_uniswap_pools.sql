
 {{
  config(
        schema='arrakis_optimism',
        alias='uniswap_pools',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['lp_name', 'contract_address', 'pool'],
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "project",
                                    "arrakis_finance",
                                    \'["msilb7"]\') }}'
  )
}}

SELECT 
    'optimism' AS blockchain,

    CONCAT(COALESCE(e0.symbol,token0)
            , '/'
            ,COALESCE(e1.symbol,token1)
            ,'-'
            , TRIM(CAST(CAST(ROUND(fee / 1e4, 2) AS DECIMAL(20, 2)) AS VARCHAR))
            ,'%'
            ,'-'
            ,CAST( ROW_NUMBER() OVER (PARTITION BY uniPool ORDER BY pc.evt_block_time ASC) AS VARCHAR)
            )
    AS lp_name,
    
    pc.pool AS contract_address, uniPool as pool, fee, token0, token1
FROM {{ source('arrakis_optimism', 'ArrakisFactoryV1_evt_PoolCreated') }} pc 
    INNER JOIN {{ ref('uniswap_optimism_pools') }} up 
        ON up.pool = pc.uniPool
    LEFT JOIN {{ ref('tokens_erc20') }} e0 
        ON e0.contract_address = up.token0
        AND e0.blockchain = 'optimism'
    LEFT JOIN {{ ref('tokens_erc20') }} e1
        ON e1.contract_address = up.token1
        AND e1.blockchain = 'optimism'