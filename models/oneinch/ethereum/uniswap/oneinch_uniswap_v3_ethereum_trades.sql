{{  config(
        tags = ['dunesql'],
        schema='oneinch_uniswap_v3_ethereum',
        alias = alias('trades'),
        partition_by = ['block_month'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2021-11-07' %} --for testing, use small subset of data
{% set burn_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %} --according to etherscan label
{% set blockchain = 'ethereum' %}
{% set blockchain_symbol = 'ETH' %}

WITH uniswap AS
(
    SELECT
        call_block_number,
        output_returnAmount,
        amount,
        pools,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        contract_address
    FROM
        {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_uniswapV3Swap') }}
    WHERE
        call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' DAY)
        {% else %}
        AND call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        
    UNION ALL

    SELECT
        call_block_number,
        output_returnAmount,
        amount,
        pools,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        contract_address
    FROM
        {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_uniswapV3SwapTo') }}
    WHERE
        call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' DAY)
        {% else %}
        AND call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        
    UNION ALL

    SELECT
        call_block_number,
        output_returnAmount,
        amount,
        pools,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        contract_address
    FROM
        {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_uniswapV3SwapToWithPermit') }}
    WHERE
        call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' DAY)
        {% else %}
        AND call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        
    UNION ALL

    SELECT
        call_block_number,
        output_returnAmount,
        amount,
        pools,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'AggregationRouterV5_call_uniswapV3Swap') }}
    WHERE
        call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' DAY)
        {% else %}
        AND call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        
    UNION ALL

    SELECT
        call_block_number,
        output_returnAmount,
        amount,
        pools,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'AggregationRouterV5_call_uniswapV3SwapTo') }}
    WHERE
        call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' DAY)
        {% else %}
        AND call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        
    UNION ALL

    SELECT
        call_block_number,
        output_returnAmount,
        amount,
        pools,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        contract_address
    FROM
        {{ source('oneinch_ethereum', 'AggregationRouterV5_call_uniswapV3SwapToWithPermit') }}
    WHERE
        call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' DAY)
        {% else %}
        AND call_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
)
, token_bought as
(
    SELECT
        call_block_number
        , output_returnAmount
        , amount
        , CASE
            WHEN CAST(pools[cardinality(pools)] / POWER(2, 252) AS INTEGER) = bitwise_and(CAST(pools[cardinality(pools)] / POWER(2, 252) AS INTEGER), 2) AND CAST(pools[cardinality(pools)] / POWER(2, 252) AS INTEGER) != 0
            THEN {{burn_address}}
            ELSE to
        END as dstToken
        , pools
        , call_tx_hash
        , call_trace_address
        , call_block_time
        , contract_address
    FROM
    (
        SELECT
            uniswap.call_block_number
            , uniswap.output_returnAmount
            , uniswap.amount
            , uniswap.pools
            , uniswap.call_tx_hash
            , uniswap.call_trace_address
            , uniswap.call_block_time
            , uniswap.contract_address
            , traces.to
            , traces."from"
            , ROW_NUMBER() OVER (
                PARTITION BY uniswap.call_tx_hash, uniswap.call_trace_address
                ORDER BY traces.trace_address desc
                ) as first_transfer_trace
        FROM
            uniswap
        LEFT JOIN
            {{ source('ethereum', 'traces') }} AS traces
            ON traces.tx_hash = uniswap.call_tx_hash
            AND traces.block_number = uniswap.call_block_number
            AND traces."from" != uniswap.contract_address
            AND COALESCE(uniswap.call_trace_address, ARRAY[]) = SLICE(traces.trace_address, 1, COALESCE(cardinality(uniswap.call_trace_address), 0))
            AND COALESCE(cardinality(uniswap.call_trace_address), 0) + 2 = COALESCE(cardinality(traces.trace_address), 0)
            AND bytearray_substring(traces.input,1,4) = 0xa9059cbb --find the token address that transfer() was called on
            AND traces.call_type = 'call'
            {% if is_incremental() %}
            AND traces.block_time >= date_trunc('day', now() - interval '7' DAY)
            {% else %}
            AND traces.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
    )
    WHERE first_transfer_trace = 1
)
, token_sold as
(
    SELECT
        call_block_number
        , output_returnAmount
        , amount
        , COALESCE(to, {{burn_address}}) AS srcToken
        , pools
        , call_tx_hash
        , call_trace_address
        , call_block_time
        , contract_address
    FROM
    (
        SELECT
            uniswap.call_block_number
            , uniswap.output_returnAmount
            , uniswap.amount
            , uniswap.pools
            , uniswap.call_tx_hash
            , uniswap.call_trace_address
            , uniswap.call_block_time
            , uniswap.contract_address
            , traces.to
            , traces."from"
            , ROW_NUMBER() OVER (
                PARTITION BY uniswap.call_tx_hash, uniswap.call_trace_address
                ORDER BY traces.trace_address
                ) as first_transfer_trace
        FROM
            uniswap
        LEFT JOIN
            {{ source('ethereum', 'traces') }} AS traces
            ON traces.tx_hash = uniswap.call_tx_hash
            AND traces.block_number = uniswap.call_block_number
            AND COALESCE(uniswap.call_trace_address, ARRAY[]) = SLICE(traces.trace_address, 1, COALESCE(cardinality(uniswap.call_trace_address), 0))
            AND COALESCE(cardinality(uniswap.call_trace_address), 0) + 3 = COALESCE(cardinality(traces.trace_address), 0)
            AND bytearray_substring(traces.input,1,4) = 0x23b872dd --find the token address that transfer() was called on
            AND traces.call_type = 'call'
            {% if is_incremental() %}
            AND traces.block_time >= date_trunc('day', now() - interval '7' DAY)
            {% else %}
            AND traces.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
    )
    WHERE first_transfer_trace = 1
)
, oneinch as
(
    SELECT
        src.call_block_number as block_number
        ,src.call_block_time as block_time
        ,'1inch' AS project
        ,'UNI v3' AS version
        ,CAST(NULL as VARBINARY) AS taker
        ,CAST(NULL as VARBINARY) as maker
        ,src.output_returnAmount AS token_bought_amount_raw
        ,src.amount AS token_sold_amount_raw
        ,CAST(NULL as double) AS amount_usd
        ,token_bought.dstToken AS token_bought_address
        ,token_sold.srcToken AS token_sold_address
        ,src.contract_address AS project_contract_address
        ,src.call_tx_hash as tx_hash
        ,src.call_trace_address AS trace_address
        ,CAST(-1 as integer) AS evt_index
    FROM
        uniswap as src
    INNER JOIN token_bought
        ON src.call_tx_hash = token_bought.call_tx_hash
        AND src.call_block_number = token_bought.call_block_number
        AND src.call_trace_address = token_bought.call_trace_address
    INNER JOIN token_sold
        ON src.call_tx_hash = token_sold.call_tx_hash
        AND src.call_block_number = token_sold.call_block_number
        AND src.call_trace_address = token_sold.call_trace_address
)
SELECT
    '{{blockchain}}' AS blockchain
    ,src.project
    ,src.version
    ,CAST(date_trunc('day', src.block_time) as date) AS block_date
    ,CAST(date_trunc('month', src.block_time) as date) AS block_month
    ,src.block_time
    ,src.block_number
    ,token_bought.symbol AS token_bought_symbol
    ,token_sold.symbol AS token_sold_symbol
    ,case
        when lower(token_bought.symbol) > lower(token_sold.symbol) then concat(token_sold.symbol, '-', token_bought.symbol)
        else concat(token_bought.symbol, '-', token_sold.symbol)
    end as token_pair
    ,src.token_bought_amount_raw / power(10, token_bought.decimals) AS token_bought_amount
    ,src.token_sold_amount_raw / power(10, token_sold.decimals) AS token_sold_amount
    ,src.token_bought_amount_raw
    ,src.token_sold_amount_raw
    ,coalesce(
        src.amount_usd
        , (src.token_bought_amount_raw / power(10,
            CASE
                WHEN src.token_bought_address = {{burn_address}}
                    THEN 18
                ELSE prices_bought.decimals
            END
            )
        )
        *
        (
            CASE
                WHEN src.token_bought_address = {{burn_address}}
                    THEN prices_eth.price
                ELSE prices_bought.price
            END
        )
        , (src.token_sold_amount_raw / power(10,
            CASE
                WHEN src.token_sold_address = {{burn_address}}
                    THEN 18
                ELSE prices_sold.decimals
            END
            )
        )
        *
        (
            CASE
                WHEN src.token_sold_address = {{burn_address}}
                    THEN prices_eth.price
                ELSE prices_sold.price
            END
        )
    ) AS amount_usd
    ,src.token_bought_address
    ,src.token_sold_address
    ,coalesce(src.taker, tx."from") AS taker
    ,src.maker
    ,src.project_contract_address
    ,src.tx_hash
    ,tx."from" AS tx_from
    ,tx.to AS tx_to
    ,src.trace_address
    ,src.evt_index
FROM
    oneinch as src
INNER JOIN {{ source('ethereum', 'transactions') }} as tx
    ON src.tx_hash = tx.hash
    AND src.block_number = tx.block_number
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' DAY)
    {% else %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} as token_bought
    ON token_bought.contract_address = src.token_bought_address
    AND token_bought.blockchain = '{{blockchain}}'
LEFT JOIN {{ ref('tokens_erc20') }} as token_sold
    ON token_sold.contract_address = src.token_sold_address
    AND token_sold.blockchain = '{{blockchain}}'
LEFT JOIN {{ source('prices', 'usd') }} as prices_bought
    ON prices_bought.minute = date_trunc('minute', src.block_time)
    AND prices_bought.contract_address = src.token_bought_address
    AND prices_bought.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND prices_bought.minute >= date_trunc('day', now() - interval '7' DAY)
    {% else %}
    AND prices_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} as prices_sold
    ON prices_sold.minute = date_trunc('minute', src.block_time)
    AND prices_sold.contract_address = src.token_sold_address
    AND prices_sold.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND prices_sold.minute >= date_trunc('day', now() - interval '7' DAY)
    {% else %}
    AND prices_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} as prices_eth
    ON prices_eth.minute = date_trunc('minute', src.block_time)
    AND prices_eth.blockchain is null
    AND prices_eth.symbol = '{{blockchain_symbol}}'
    {% if is_incremental() %}
    AND prices_eth.minute >= date_trunc('day', now() - interval '7' DAY)
    {% else %}
    AND prices_eth.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}