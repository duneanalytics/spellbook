{{  config(
        schema='oneinch_unoswap_v4_ethereum',
        alias = alias('trades'),
        partition_by = ['block_date'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2021-11-07' %} --for testing, use small subset of data
{% set generic_null_address = '0x0000000000000000000000000000000000000000' %} --according to etherscan label
{% set burn_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %} --according to etherscan label
{% set blockchain = 'ethereum' %}
{% set blockchain_symbol = 'ETH' %}

WITH unoswap AS
(
    SELECT
        call_block_number,
        output_returnAmount,
        amount,
        srcToken,
        pools,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        contract_address
    FROM
        {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_unoswap') }}
    WHERE
        call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        AND call_block_time >= '{{project_start_date}}'
        {% endif %}

    UNION ALL

    SELECT
        call_block_number,
        output_returnAmount,
        amount,
        srcToken,
        pools,
        call_tx_hash,
        call_trace_address,
        call_block_time,
        contract_address
    FROM
        {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_unoswapWithPermit') }}
    WHERE
        call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        AND call_block_time >= '{{project_start_date}}'
        {% endif %}
)
, oneinch as
(
    SELECT
        src.call_block_number as block_number,
        src.call_block_time as block_time,
        '1inch' AS project,
        'UNI v2' AS version,
        tx.from AS taker,
        CAST(NULL as string) as maker,
        src.output_returnAmount AS token_bought_amount_raw,
        src.amount AS token_sold_amount_raw,
        CAST(NULL as double) AS amount_usd,
        CASE
            WHEN ll.to = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                AND SUBSTRING(src.pools[ARRAY_SIZE(src.pools) - 1], 1, 4) IN ('0xc0', '0x40') --spark uses 0-based array index, subtract 1 from size output
            THEN '{{burn_address}}'
            ELSE ll.to
        END AS token_bought_address,
        CASE
            WHEN src.srcToken = '{{generic_null_address}}'
            THEN '{{burn_address}}'
            ELSE src.srcToken
        END AS token_sold_address,
        src.contract_address AS project_contract_address,
        src.call_tx_hash as tx_hash,
        src.call_trace_address AS trace_address,
        CAST(-1 as integer) AS evt_index,
        tx.from AS tx_from,
        tx.to AS tx_to
    FROM
        unoswap as src
    INNER JOIN {{ source('ethereum', 'transactions') }} as tx
        ON src.call_tx_hash = tx.hash
        AND src.call_block_number = tx.block_number
        {% if is_incremental() %}
        AND tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        AND tx.block_time >= '{{project_start_date}}'
        {% endif %}
    LEFT JOIN {{ source('ethereum', 'traces') }} as ll
        ON src.call_tx_hash = ll.tx_hash
        AND src.call_block_number = ll.block_number
        AND ll.trace_address = (
            CONCAT(
                src.call_trace_address,
                ARRAY
                (
                    ARRAY_SIZE(src.pools)
                    * 2 
                    + CASE
                        WHEN src.srcToken = '{{generic_null_address}}'
                        THEN 1
                        ELSE 0
                    END
                ),
                ARRAY(0)
            )
        )
        {% if is_incremental() %}
        AND ll.block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        AND ll.block_time >= '{{project_start_date}}'
        {% endif %}
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
    ,src.taker
    ,src.maker
    ,src.project_contract_address
    ,src.tx_hash
    ,src.tx_from
    ,src.tx_to
    ,CAST(src.trace_address as array<long>) as trace_address
    ,src.evt_index
FROM
    oneinch as src
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