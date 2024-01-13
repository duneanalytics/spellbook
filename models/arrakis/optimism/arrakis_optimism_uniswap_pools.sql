
 {{
  config(
        
        schema='arrakis_optimism',
        alias = 'uniswap_pools',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['contract_address', 'pool_contract'],
  )
}}

SELECT distinct
    'optimism' AS blockchain,

    CONCAT(COALESCE(e0.symbol,cast(token0 as varchar))
            , '/'
            ,COALESCE(e1.symbol,cast(token1 as varchar))
            ,'-'
            , TRIM(CAST(CAST(ROUND(fee / 1e4, 2) AS DECIMAL(20, 2)) AS VARCHAR(10) ))
            ,'%'
            ,'-'
            ,CAST( ROW_NUMBER() OVER (PARTITION BY uniPool ORDER BY pc.evt_block_time ASC) AS VARCHAR(10) )
            )
    AS lp_name,
    
    pc.pool AS contract_address, uniPool as pool_contract, fee, token0, token1

FROM {{ source('arrakis_optimism', 'ArrakisFactoryV1_evt_PoolCreated') }} pc 
    INNER JOIN {{ ref('uniswap_optimism_pools') }} up 
        ON up.pool = pc.uniPool
    LEFT JOIN {{ source('tokens', 'erc20') }} e0
        ON e0.contract_address = up.token0
        AND e0.blockchain = 'optimism'
    LEFT JOIN {{ source('tokens', 'erc20') }} e1
        ON e1.contract_address = up.token1
        AND e1.blockchain = 'optimism'

{% if is_incremental() %}
WHERE pc.evt_block_time >= date_trunc('day', now() - interval '1' month)
{% endif %}