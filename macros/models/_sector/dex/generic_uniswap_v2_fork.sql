{% macro generic_uniswap_v2_fork(
    blockchain = null
    , transactions = null
    , logs = null
    , contracts = null
    )
%}

with decoding_raw_forks as 
(
    Select 
    contract_address
    ,tx_hash as evt_tx_hash
    ,index as evt_index
    ,block_time as evt_block_time
    ,block_number as evt_block_number
    ,varbinary_to_uint256(varbinary_substring(data, 1, 32)) as amount0In  
    ,varbinary_to_uint256(varbinary_substring(data, 33, 32)) as amount1In
    ,varbinary_to_uint256(varbinary_substring(data, 66, 32)) as amount0Out
    ,varbinary_to_uint256(varbinary_substring(data, 99, 32)) as amount1Out
    ,varbinary_substring(topic1, 13, 20) as sender
    ,varbinary_substring(topic2, 13, 20) as to
from  {{logs}}
    where topic0 = 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822 --topic0 for uniswap_v2 swap event
)

,dexs AS
(
-- Uniswap v2
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.to AS taker
        , t.contract_address AS maker
        , amount0Out AS token_bought_amount_raw
        , amount1In AS token_sold_amount_raw
        , f.token0 AS token_bought_address
        , f.token1 AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
        , f.factory_address 
    FROM decoding_raw_forks t
    INNER JOIN (Select 
                contract_address as factory_address
                 ,VARBINARY_SUBSTRING(data, 13,20) as pair
                 ,VARBINARY_SUBSTRING(topic1, 13, 20) AS token0
                 ,VARBINARY_SUBSTRING(topic2, 13, 20) AS token1 
                from  {{logs}}
                where topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9 --topic0 for uniswap_v2 factory event Pair_created
             ) f
     ON f.pair = t.contract_address
        WHERE   (amount0Out > UINT256 '0' OR amount1In > UINT256 '0')
        AND t.contract_address NOT IN (SELECT address FROM {{contracts}}) --excluding already decoded contracts to avoid duplicates in dex.trades
        {% if is_incremental() %}
        AND {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}

UNION ALL

    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.to AS taker
        , t.contract_address AS maker
        , amount1Out AS token_bought_amount_raw
        , amount0In AS token_sold_amount_raw
        , f.token1 AS token_bought_address
        , f.token0 AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
        , f.factory_address 
    FROM
        decoding_raw_forks t
    INNER JOIN (Select 
                contract_address as factory_address
                 ,VARBINARY_SUBSTRING(data, 13,20) as pair
                 ,VARBINARY_SUBSTRING(topic1, 13, 20) AS token0
                 ,VARBINARY_SUBSTRING(topic2, 13, 20) AS token1 
                from  {{logs}}
                where topic0 = 0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9 --topic0 for uniswap_v2 factory event Pair_created
             ) f
    ON f.pair = t.contract_address
        WHERE   (amount1Out > UINT256 '0' OR amount0In > UINT256 '0')
        AND t.contract_address NOT IN (SELECT address FROM {{contracts}}) --excluding already decoded contracts to avoid duplicates in dex.trades
        {% if is_incremental() %}
        AND {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}
)


SELECT
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , CAST(dexs.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw
    , CAST(dexs.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM
    dexs
LEFT JOIN {{ref('dex_uniswap_v2_fork_mapping') }} fac
    ON dexs.deployed_by_contract_address = fac.factory_address

{% endmacro %}


