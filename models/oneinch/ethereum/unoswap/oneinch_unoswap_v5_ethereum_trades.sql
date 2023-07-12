{{  config(
        schema='oneinch_unoswap_v5_ethereum',
        alias='trades',
        partition_by = ['block_date'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2022-11-07' %} --for testing, use small subset of data
{% set generic_null_address = '0x0000000000000000000000000000000000000000' %} --according to etherscan label
{% set burn_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %} --according to etherscan label
{% set blockchain = 'ethereum' %}
{% set blockchain_symbol = 'ETH' %}

WITH unoswap AS
(
    SELECT
        call_block_number
        ,output_returnAmount
        ,amount
        ,srcToken
        ,pools
        ,call_tx_hash
        ,call_trace_address
        ,call_block_time
        ,contract_address
    FROM
        {{ source('oneinch_ethereum', 'AggregationRouterV5_call_unoswap') }}
    WHERE
        call_success
        /******************************************************************************************************************
            - a few tx's don't fit into the join on line 124:
                AND COALESCE(array_size(unoswap.call_trace_address), 0) + 2 = COALESCE(array_size(traces.trace_address), 0)
            - the '+ 2' should apparently be '+ 3' for these tx's to correctly join to traces
            - on v1 engine, the tx's were forced to amount_usd = 0 via update statement, as full refresh is less common there
        ********************************************************************************************************************/
        AND call_tx_hash not in (
            '0x4f98ac5d5778203a0df3848c85494a179eae35befa64bb6fc360f03851385191'
            , '0xce87a97efbf1c6c0491a72997d5239029ced77c9ef7413db66cc30b4da63fe86'
            , '0x62c833c1ab66d17c42aeb1407755c00894f9af8691da2c2ca0f14392e3a6334c'
            , '0x774ad4c15a6f776e71641fe4e9af3abd5bb80f7511c77548d130c2ee124ba80a'
            , '0xad7d5814544440bdcb22760f8f2f0594958e9e6417249d96d92bf78cd05a80f5'
            , '0xafba4b3db26b0e9f26d0ca4c709e80ee2b8bc18e3298fa67126697fc45fba0c6'
        )
        /***************************************************
            remove tx_hash filter if join is fixed
        ***************************************************/
        {% if is_incremental() %}
        AND call_block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        AND call_block_time >= '{{project_start_date}}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_number
        ,output_returnAmount
        ,amount
        ,srcToken
        ,pools
        ,call_tx_hash
        ,call_trace_address
        ,call_block_time
        ,contract_address
    FROM
        {{ source('oneinch_ethereum', 'AggregationRouterV5_call_unoswapToWithPermit') }}
    WHERE
        call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        AND call_block_time >= '{{project_start_date}}'
        {% endif %}
)
, join_to_traces as
(
    SELECT
        call_block_number
        , output_returnAmount
        , amount
        , srcToken
        , CASE
            WHEN CAST(pools[array_size(pools) - 1] / POWER(2, 252) as int) & 2 != 0
            THEN '{{burn_address}}'
            ELSE to
        END as dstToken
        , pools
        , call_tx_hash
        , call_trace_address
        , call_block_time
        , contract_address
        , from as taker
    FROM
    (
        SELECT
            unoswap.call_block_number
            , unoswap.output_returnAmount
            , unoswap.amount
            , unoswap.srcToken
            , unoswap.pools
            , unoswap.call_tx_hash
            , unoswap.call_trace_address
            , unoswap.call_block_time
            , unoswap.contract_address
            , traces.to
            , traces.from
            , ROW_NUMBER() OVER (
                PARTITION BY unoswap.call_tx_hash, unoswap.call_trace_address
                ORDER BY traces.trace_address desc
                ) as first_transfer_trace
        FROM
            unoswap
        LEFT JOIN
            {{ source('ethereum', 'traces') }} AS traces
            ON traces.tx_hash = unoswap.call_tx_hash
            AND traces.block_number = unoswap.call_block_number
            AND traces.from != unoswap.contract_address
            AND COALESCE(unoswap.call_trace_address, CAST(ARRAY() as array<bigint>)) = SLICE(traces.trace_address, 1, COALESCE(array_size(unoswap.call_trace_address), 0))
            AND COALESCE(array_size(unoswap.call_trace_address), 0) + 2 = COALESCE(array_size(traces.trace_address), 0)
            AND SUBSTRING(traces.input,1,10) = '0xa9059cbb' --find the token address that transfer() was called on
            AND traces.call_type = 'call'
            {% if is_incremental() %}
            AND traces.block_time >= date_trunc("day", now() - interval '1 week')
            {% else %}
            AND traces.block_time >= '{{project_start_date}}'
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
        ,'UNI v2' AS version
        ,CAST(NULL as string) AS taker
        ,CAST(NULL as string) as maker
        ,src.output_returnAmount AS token_bought_amount_raw
        ,src.amount AS token_sold_amount_raw
        ,CAST(NULL as double) AS amount_usd
        ,src.dstToken AS token_bought_address
        ,CASE
            WHEN src.srcToken = '{{generic_null_address}}'
            THEN '{{burn_address}}'
            ELSE src.srcToken
        END AS token_sold_address
        ,src.contract_address AS project_contract_address
        ,src.call_tx_hash as tx_hash
        ,src.call_trace_address AS trace_address
        ,CAST(-1 as integer) AS evt_index
    FROM
        join_to_traces as src
)
SELECT
    '{{blockchain}}' AS blockchain
    ,src.project
    ,src.version
    ,date_trunc('day', src.block_time) AS block_date
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
    ,CAST(src.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw
    ,CAST(src.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw
    ,coalesce(
        src.amount_usd
        , (src.token_bought_amount_raw / power(10,
            CASE
                WHEN src.token_bought_address = '{{burn_address}}'
                    THEN 18
                ELSE prices_bought.decimals
            END
            )
        )
        *
        (
            CASE
                WHEN src.token_bought_address = '{{burn_address}}'
                    THEN prices_eth.price
                ELSE prices_bought.price
            END
        )
        , (src.token_sold_amount_raw / power(10,
            CASE
                WHEN src.token_sold_address = '{{burn_address}}'
                    THEN 18
                ELSE prices_sold.decimals
            END
            )
        )
        *
        (
            CASE
                WHEN src.token_sold_address = '{{burn_address}}'
                    THEN prices_eth.price
                ELSE prices_sold.price
            END
        )
    ) AS amount_usd
    ,src.token_bought_address
    ,src.token_sold_address
    ,coalesce(src.taker, tx.from) AS taker
    ,src.maker
    ,src.project_contract_address
    ,src.tx_hash
    ,tx.from AS tx_from
    ,tx.to AS tx_to
    ,CAST(src.trace_address as array<long>) as trace_address
    ,src.evt_index
FROM
    oneinch as src
INNER JOIN {{ source('ethereum', 'transactions') }} as tx
    ON src.tx_hash = tx.hash
    AND src.block_number = tx.block_number
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20_legacy') }} as token_bought
    ON token_bought.contract_address = src.token_bought_address
    AND token_bought.blockchain = '{{blockchain}}'
LEFT JOIN {{ ref('tokens_erc20_legacy') }} as token_sold
    ON token_sold.contract_address = src.token_sold_address
    AND token_sold.blockchain = '{{blockchain}}'
LEFT JOIN {{ source('prices', 'usd') }} as prices_bought
    ON prices_bought.minute = date_trunc('minute', src.block_time)
    AND prices_bought.contract_address = src.token_bought_address
    AND prices_bought.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND prices_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND prices_bought.minute >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} as prices_sold
    ON prices_sold.minute = date_trunc('minute', src.block_time)
    AND prices_sold.contract_address = src.token_sold_address
    AND prices_sold.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND prices_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND prices_sold.minute >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} as prices_eth
    ON prices_eth.minute = date_trunc('minute', src.block_time)
    AND prices_eth.blockchain is null
    AND prices_eth.symbol = '{{blockchain_symbol}}'
    {% if is_incremental() %}
    AND prices_eth.minute >= date_trunc("day", now() - interval '1 week')
    {% else %}
    AND prices_eth.minute >= '{{project_start_date}}'
    {% endif %}
;