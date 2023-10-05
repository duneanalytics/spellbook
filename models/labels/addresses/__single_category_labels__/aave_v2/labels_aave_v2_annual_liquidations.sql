{{config(
    tags=['dunesql']
    , alias = alias('aave_v2_liquidations')
)}}


WITH mev_addresses AS (
    SELECT 
        address
    FROM {{ ref('labels_mev_ethereum') }}
)

, aave_v2_annual_liquidations AS (
    SELECT 
        * 
    FROM {{ source('aave_v2_ethereum', 'LendingPool_evt_LiquidationCall') }}
    WHERE evt_block_time > now() - INTERVAL '365' day
)

, all_liquidations AS (
SELECT
    a.evt_block_time
    , a.evt_tx_hash
    , a.collateralAsset
    , cast(a.liquidatedCollateralAmount as double) / pow(10, p.decimals) AS liquidatedCollateralAmount
    , p.symbol
    , m.address
    , m.address AS liquidator
FROM aave_v2_annual_liquidations a
LEFT JOIN mev_addresses m ON m.address = a.liquidator
LEFT JOIN {{ ref('prices_tokens') }} p
    ON p.contract_address = a.collateralAsset
    AND p.blockchain = 'ethereum'
)

, final_base_label AS (
SELECT
    'ethereum' AS blockchain
    , address AS address
    , case when liquidator is not null then 'MEV'
        else 'non-MEV'
        end as name 
    , 'liquidation type' AS category
    , 'paulx' AS contributor
    , 'query' AS source
    , date '2023-03-12' AS created_at
    , now() AS updated_at
    , 'aave_v2 annual liquidations' AS model_name
    , 'persona' AS label_type
    , evt_block_time
    , evt_tx_hash
    , liquidatedCollateralAmount
    , symbol
FROM all_liquidations
)

SELECT
    blockchain
    , address
    , name
    , category
    , contributor
    , source
    , created_at
    , updated_at
    , model_name
    , label_type
FROM final_base_label
