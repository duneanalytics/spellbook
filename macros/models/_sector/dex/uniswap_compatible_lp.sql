{% macro uniswap_compatible_v3_lp(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Mint = null
    , Pair_evt_Burn = null
    , Factory_evt_PoolCreated = null
    , NonfungibleTokenPositionManager_evt_Transfer = null
    , NonfungibleTokenPositionManager_evt_IncreaseLiquidity = null
    , NonfungibleTokenPositionManager_evt_DecreaseLiquidity = null
    , position_manager_addr = null
    )
%}
WITH id_to_lp AS
(
    SELECT
        distinct t.tokenId
        ,t.to AS lp_address
    FROM
        {{ NonfungibleTokenPositionManager_evt_Transfer }}
    WHERE
        t."from" = 0x0000000000000000000000000000000000000000
)

, mints AS
(
    SELECT
        'mint' AS event_type
        ,m.evt_block_number AS block_number
        ,m.evt_block_time AS block_time
        ,m.evt_tx_hash AS tx_hash
        ,m.evt_index
        , {% if position_manager_addr %}
                id.lp_address
            {% else %}
                m.owner
            {% endif %} AS lp_address
        , {% if position_manager_addr %}
                cast(pm.tokenId AS double)
            {% else %}
                0
            {% endif %} AS position_id
        , m.tickLower AS tick_lower
        , m.tickUpper AS tick_upper
        , m.amount AS liquidity
        , m.amount0 AS amount0
        , m.amount1 AS amount1
        , f.token0 AS token0_address
        , f.token1 AS token1_address
        , m.contract_address AS pool_address
    FROM
        {{ Pair_evt_Mint }} m
    INNER JOIN
        {{ Factory_evt_PoolCreated }} f
        ON f.pool = m.contract_address
    LEFT JOIN
        {{ NonfungibleTokenPositionManager_evt_IncreaseLiquidity }} pm
        ON m.owner = pm.contract_address and m.evt_tx_hash = pm.evt_tx_hash
    LEFT JOIN id_to_lp AS id
        ON pm.tokenId = id.tokenId
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('m.evt_block_time') }}
    {% endif %}
)

, burns AS
(
    SELECT
        'burn' AS event_type
        ,b.evt_block_number AS block_number
        ,b.evt_block_time AS block_time
        ,b.evt_tx_hash AS tx_hash
        ,b.evt_index
        , {% if position_manager_addr %}
                id.lp_address
            {% else %}
                b.owner
            {% endif %} AS lp_address
        , {% if position_manager_addr %}
                cast(pm.tokenId AS double)
            {% else %}
                0
            {% endif %} AS position_id
        , b.tickLower AS tick_lower
        , b.tickUpper AS tick_upper
        , b.amount AS liquidity
        , b.amount0
        , b.amount1
        , f.token0 AS token0_address
        , f.token1 AS token1_address
        , m.contract_address AS pool_address
    FROM
        {{ Pair_evt_Burn }} b
    INNER JOIN
        {{ Factory_evt_PoolCreated }} f
        ON f.pool = b.contract_address
    LEFT JOIN
        {{ NonfungibleTokenPositionManager_evt_IncreaseLiquidity }} pm
        ON b.owner = pm.contract_address and b.evt_tx_hash = pm.evt_tx_hash
    LEFT JOIN id_to_lp AS id
        ON pm.tokenId = id.tokenId
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('b.evt_block_time') }}
    {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , lp_data.event_type
    , lp_data.block_number
    , lp_data.block_time
    , date_trunc('MONTH', lp_data.block_time) AS block_month
    , lp_data.tx_hash
    , lp_data.evt_index
    , lp_data.lp_address
    , lp_data.position_id
    , lp.tick_lower
    , lp.tick_upper
    , CAST(lp_data.liquidity AS UINT256) AS liquidity_raw
    , CAST(lp_data.amount0 AS UINT256) AS amount0_raw
    , CAST(lp_data.amount1 AS UINT256) AS amount1_raw
    , lp_data.token0_address
    , lp_data.token1_address
    , lp_data.pool_address
FROM
    (
        select * from mints
        union all
        select * from burns
    ) lp_data
{% endmacro %}