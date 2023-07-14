{{  config(
        schema='oneinch_v4_ethereum',
        alias = alias('trades'),
        partition_by = ['block_date'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2021-11-08' %} --for testing, use small subset of data
{% set generic_null_address = '0x0000000000000000000000000000000000000000' %} --according to etherscan label
{% set burn_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %} --according to etherscan label
{% set blockchain = 'ethereum' %}
{% set blockchain_symbol = 'ETH' %}

{% set final_columns = [
    'block_number'
    ,'taker'
    ,'from_token'
    ,'to_token'
    ,'from_amount'
    ,'to_amount'
    ,'tx_hash'
    ,'block_time'
    ,'trace_address'
    ,'evt_index'
    ,'contract_address'
] %}

WITH oneinch_calls AS
(    
    SELECT
        call_block_number as block_number,
        CAST(NULL as string) as taker,
        get_json_object(desc,'$.srcToken') as from_token,
        get_json_object(desc,'$.dstToken') as to_token,
        output_spentAmount as from_amount,
        output_returnAmount as to_amount,
        call_tx_hash as tx_hash,
        call_block_time as block_time,
        call_trace_address as trace_address,
        CAST(-1 as integer) AS evt_index,
        contract_address
    FROM
        {{ source('oneinch_v4_ethereum', 'AggregationRouterV4_call_swap') }}
    WHERE
        call_success
        {% if is_incremental() %}
        AND call_block_time >= date_trunc("day", now() - interval '1 week')
        {% else %}
        AND call_block_time >= '{{project_start_date}}'
        {% endif %}
)
, oneinch AS
(
    SELECT
        block_number,
        block_time,
        '1inch' AS project,
        '4' as version,
        taker,
        CAST(NULL as string) AS maker,
        to_amount AS token_bought_amount_raw,
        from_amount AS token_sold_amount_raw,
        CAST(NULL as double) AS amount_usd,
        CASE
          WHEN to_token = '{{generic_null_address}}'
            THEN '{{burn_address}}'
            ELSE to_token
        END AS token_bought_address,
        CASE
          WHEN from_token = '{{generic_null_address}}'
              THEN '{{burn_address}}'
              ELSE from_token
        END AS token_sold_address,
        contract_address AS project_contract_address,
        tx_hash,
        trace_address,
        evt_index
    FROM
    (
        SELECT
            {% for column in final_columns %}
            {% if not loop.first %},{% endif %} {{column}}
            {% endfor %}
        FROM
            oneinch_calls
    )
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
                WHEN token_bought_address = '{{burn_address}}'
                    THEN 18
                ELSE prices_bought.decimals
            END
            )
        )
        *
        (
            CASE
                WHEN token_bought_address = '{{burn_address}}'
                    THEN prices_eth.price
                ELSE prices_bought.price
            END
        )
        , (src.token_sold_amount_raw / power(10,
            CASE
                WHEN token_sold_address = '{{burn_address}}'
                    THEN 18
                ELSE prices_sold.decimals
            END
            )
        )
        *
        (
            CASE
                WHEN token_sold_address = '{{burn_address}}'
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
FROM oneinch as src
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