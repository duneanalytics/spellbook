{{
    config(
        schema = 'curve_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

--see curvefi readme for more info on the logic below.
WITH dexs AS
(
    --factory v1 and regular pools
    SELECT
        l.block_number
        , l.block_time
        , p.version as version
        , bytearray_substring(l.topic1, 13, 20) as taker
        , CAST(NULL as VARBINARY) as maker
        , case
            when l.topic0 = 0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b
                        AND cast(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) as int) = 0
                then 'underlying_exchange_base'
                else 'normal_exchange'
            end as swap_type
        , bytearray_to_uint256(bytearray_substring(l.data, 97, 32)) as token_bought_amount_raw
        , bytearray_to_uint256(bytearray_substring(l.data, 33, 32)) as token_sold_amount_raw
        , cast(NULL as double) AS amount_usd
        , case
            when l.topic0 = 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140
                    and cast(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) as int) + 1 <= CARDINALITY(p.coins)
                then p.coins[cast(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) as int) + 1] 
            when l.topic0 != 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140
                    and cast(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) as int) + 1 <= CARDINALITY(p.undercoins)    
                then p.undercoins[cast(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) as int) + 1]
                else NULL
            end as token_bought_address
        , case
            when l.topic0 = 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140
                    and cast(bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) as int) + 1 <= CARDINALITY(p.coins)
                then p.coins[cast(bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) as int) + 1] 
            when l.topic0 != 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140
                    and cast(bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) as int) + 1 <= CARDINALITY(p.undercoins) 
                    then p.undercoins[cast(bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) as int) + 1]
                else NULL
            end as token_sold_address
        , l.contract_address as project_contract_address --pool address
        , l.tx_hash 
        , l.index as evt_index
    FROM {{ source('ethereum', 'logs') }} l
    JOIN  {{ ref('curve_ethereum_view_pools') }} p
        ON l.contract_address = p.pool_address
        AND p.version IN ('Factory V1 Meta', 'Factory V1 Plain', 'Regular', 'Factory V1 Stableswap Plain', 'Factory V1 Stableswap Meta', 'Factory V1 Stableswap Plain NG') --note Plain only has TokenExchange.
    WHERE l.topic0 IN
        (
            0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b -- TokenExchangeUnderlying 
            , 0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140 -- TokenExchange
        )
        {% if is_incremental() %}
        AND {{ incremental_predicate('l.block_time') }}
        {% endif %}

    UNION ALL

    --factory v2 pools and v1 plain pools have same logic
    SELECT
        l.block_number
        , l.block_time
        , p.version as version
        , bytearray_substring(l.topic1, 13, 20) as taker
        , CAST(NULL as VARBINARY) as maker
        , 'normal_exchange' as swap_type
        , bytearray_to_uint256(bytearray_substring(l.data, 97, 32)) as token_bought_amount_raw
        , bytearray_to_uint256(bytearray_substring(l.data, 33, 32)) as token_sold_amount_raw
        , cast(NULL as double) AS amount_usd
        , p.coins[cast(bytearray_to_uint256(bytearray_substring(l.data, 65, 32)) as int) + 1] as token_bought_address
        , p.coins[cast(bytearray_to_uint256(bytearray_substring(l.data, 1, 32)) as int) + 1] as token_sold_address
        , l.contract_address as project_contract_address --pool address
        , l.tx_hash 
        , l.index as evt_index
    FROM {{ source('ethereum', 'logs') }} l
    JOIN {{ ref('curve_ethereum_view_pools') }} p
        ON l.contract_address = p.pool_address
        AND (p.version = 'Factory V2' or p.version = 'Factory V2 updated' or p.version = 'Regular' or p.version = 'Factory Twocrypto') --some Regular pools are new and use the below topic instead
    WHERE (l.topic0 = 0xb2e76ae99761dc136e598d4a629bb347eccb9532a5f8bbd72e18467c3c34cc98 --TokenExchange
        OR l.topic0 = 0x143f1f8e861fbdeddd5b46e844b7d3ac7b86a122f36e8c463859ee6811b1f29c) --TokenExchange (v2 updated pool, has some other variables included after old ones so topic hash is changed.)
        {% if is_incremental() %}
        AND {{ incremental_predicate('l.block_time') }}
        {% endif %}
)
, dexs_with_decimals AS (
    SELECT
        dexs.*
        , erc20_bought.decimals as token_bought_decimals
        , erc20_sold.decimals as token_sold_decimals
        -- Calculate curve used decimals based on swap type
        , case
            when dexs.swap_type = 'underlying_exchange_base' then 18
            else coalesce(erc20_bought.decimals, 18)
        end as curve_decimals_bought
        , case
            when dexs.swap_type = 'underlying_exchange_base' then 18
            else coalesce(erc20_sold.decimals, 18)
        end as curve_decimals_sold
    FROM dexs
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_bought
        ON erc20_bought.contract_address = dexs.token_bought_address
        AND erc20_bought.blockchain = 'ethereum'
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sold
        ON erc20_sold.contract_address = dexs.token_sold_address
        AND erc20_sold.blockchain = 'ethereum'
)

SELECT
    'ethereum' AS blockchain
    ,'curve' AS project
    ,dexs_with_decimals.version AS version
    ,CAST(date_trunc('DAY', dexs_with_decimals.block_time) AS date) AS block_date
    ,CAST(date_trunc('MONTH', dexs_with_decimals.block_time) AS date) AS block_month
    ,dexs_with_decimals.block_time
    ,dexs_with_decimals.block_number
    -- Adjust raw amounts so that generic enrichment (amount_raw / 10^token_decimals) yields correct token units
    ,CAST(
        dexs_with_decimals.token_bought_amount_raw * 
        power(10, dexs_with_decimals.token_bought_decimals - dexs_with_decimals.curve_decimals_bought)
        AS UINT256
    ) as token_bought_amount_raw
    ,CAST(
        dexs_with_decimals.token_sold_amount_raw * 
        power(10, dexs_with_decimals.token_sold_decimals - dexs_with_decimals.curve_decimals_sold)
        AS UINT256
    ) as token_sold_amount_raw
    ,dexs_with_decimals.token_bought_address
    ,dexs_with_decimals.token_sold_address
    ,dexs_with_decimals.taker
    ,dexs_with_decimals.maker
    ,dexs_with_decimals.project_contract_address
    ,dexs_with_decimals.tx_hash
    ,dexs_with_decimals.evt_index
FROM dexs_with_decimals