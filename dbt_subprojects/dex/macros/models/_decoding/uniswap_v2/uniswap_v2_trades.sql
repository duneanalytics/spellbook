{% macro uniswap_v2_forks_trades(
    blockchain = null
    , dex_type = 'uni-v2'
    , version = null
    , Pair_evt_Swap = null
    , Factory_evt_PairCreated = null
    , pair_column_name = 'pair'
    )
%}

WITH evt_swap AS (
    SELECT
        block_number
        , block_time
        , to
        , contract_address
        , tx_hash
        , index
        , amount0In
        , amount0Out
        , amount1In
        , amount1Out
    FROM {{ Pair_evt_Swap }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
)

, dexs AS
(
    SELECT
        t.block_number
        , t.block_time
        , t.to AS taker
        , t.contract_address AS maker
        , CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
        , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        , CASE WHEN amount0Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_bought_address
        , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.tx_hash
        , t.index AS evt_index
        , f.contract_address as factory_address
    FROM
        evt_swap t
    INNER JOIN
        {{ Factory_evt_PairCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
    INNER JOIN {{ source(blockchain, 'creation_traces') }} ct
        ON f.{{ pair_column_name }} = ct.address
        AND f.contract_address = ct."from"
)

, base_trades AS (
    SELECT
        '{{ blockchain }}' AS blockchain
        , '{{ version }}' AS version
        , '{{dex_type}}' AS dex_type
        , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
        , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
        , dexs.block_time
        , dexs.block_number
        , dexs.token_bought_amount_raw
        , dexs.token_sold_amount_raw
        , dexs.token_bought_address
        , dexs.token_sold_address
        , dexs.taker
        , dexs.maker
        , dexs.project_contract_address
        , dexs.tx_hash
        , dexs.evt_index
        , dexs.factory_address
    FROM
        dexs
)

SELECT  base_trades.blockchain
        , CASE when dex_map.project_name is not NULL then dex_map.project_name else concat('unknown-uni-v2-', cast(varbinary_substring(factory_address, 1, 5) as varchar)) end as project_name
        , CASE when dex_map.project_name is not NULL then 'identified' else 'unidentified' end as project_name_status
        , base_trades.version
        , base_trades.dex_type
        , base_trades.factory_address
        , base_trades.block_month
        , base_trades.block_date
        , base_trades.block_time
        , base_trades.block_number
        , base_trades.token_bought_amount_raw
        , base_trades.token_sold_amount_raw
        , base_trades.token_bought_address
        , base_trades.token_sold_address
        , base_trades.taker
        , base_trades.maker
        , base_trades.project_contract_address
        , base_trades.tx_hash
        , base_trades.evt_index
FROM base_trades
INNER JOIN (
    SELECT
        tx_hash,
        array_agg(DISTINCT contract_address) as contract_addresses
    FROM {{ source('tokens', 'transfers') }}
    WHERE blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
    GROUP BY tx_hash
) AS transfers
ON transfers.tx_hash = base_trades.tx_hash
    AND contains(transfers.contract_addresses, base_trades.token_bought_address)
    AND contains(transfers.contract_addresses, base_trades.token_sold_address)
LEFT JOIN {{ ref('dex_mapping') }} AS dex_map
ON base_trades.factory_address = dex_map.factory_address
  AND base_trades.blockchain = dex_map.blockchain
where block_date >= '2024-06-01'

{% endmacro %}
