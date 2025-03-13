{{ config(
    schema = 'rollup_economics_ethereum'
    , alias = 'l2_revenue'
    , materialized = 'table'
    , file_format = 'delta'
    , full_refresh = true
    , post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "rollup_economics",
                                    \'["niftytable", "maybeYonas", "lgingerich"]\') }}'
)}}

SELECT
    date_trunc('day', block_time) AS day
    , CASE
        WHEN blockchain = 'zksync' THEN 'zksync era'
        WHEN blockchain = 'zkevm' THEN 'polygon zkevm'
        WHEN blockchain = 'optimism' THEN 'op mainnet'
        ELSE blockchain
    END AS name
    , SUM(tx_fee) AS l2_rev
    , SUM(tx_fee_usd) AS l2_rev_usd
FROM {{ source('gas', 'fees')}}
WHERE blockchain IN ('arbitrum', 'base', 'blast', 'linea', 'mantle', 'optimism', 'scroll', 'zksync', 'zkevm', 'zora')
GROUP BY 1, 2