{{ config(
    schema = 'clipper_coves_v1_polygon',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    )
}}

{% set project_start_date = '2022-05-27' %}

WITH event_data as (
    SELECT
        evt_block_time AS block_time,
        evt_block_number as block_number,
        recipient as taker,
        '' AS maker,
        CAST(inAmount AS DECIMAL(38,0)) AS token_sold_amount_raw,
        CAST(outAmount AS DECIMAL(38,0)) AS token_bought_amount_raw,
        inAsset as token_sold_address,
        outAsset as token_bought_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM  {{ source('clipper_polygon', 'ClipperCove_evt_CoveSwapped') }}
    WHERE 1=1
    {% if not is_incremental() %}
    AND evt_block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT
    'polygon' AS blockchain
    ,'clipper' AS project
    ,'coves1' AS version
    ,TRY_CAST(date_trunc('DAY', e.block_time) AS date) AS block_date
    ,e.block_time
    ,t_bought.symbol AS token_bought_symbol
    ,t_sold.symbol AS token_sold_symbol
    ,case
        when lower(t_bought.symbol) > lower(t_sold.symbol) then concat(t_sold.symbol, '-', t_bought.symbol)
        else concat(t_bought.symbol, '-', t_sold.symbol)
    end as token_pair
    ,CAST(e.token_bought_amount_raw AS DECIMAL(38,0)) / power(10, t_bought.decimals) AS token_bought_amount
    ,CAST(e.token_sold_amount_raw AS DECIMAL(38,0)) / power(10, t_sold.decimals) AS token_sold_amount
    ,CAST(e.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw
    ,CAST(e.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw
    ,coalesce(
        (CAST(e.token_bought_amount_raw AS DECIMAL(38,0)) / power(10, p_bought.decimals)) * p_bought.price
        ,(CAST(e.token_sold_amount_raw AS DECIMAL(38,0)) / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd
    ,e.token_bought_address
    ,e.token_sold_address
    ,e.taker
    ,e.maker
    ,e.project_contract_address
    ,e.tx_hash
    ,tx.from AS tx_from
    ,tx.to AS tx_to
    ,e.trace_address
    ,e.evt_index
FROM event_data e
INNER JOIN {{ source('polygon', 'transactions') }} tx
    ON tx.block_number = e.block_number
    AND tx.hash = e.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} t_bought
    ON t_bought.contract_address = e.token_bought_address
    AND t_bought.blockchain = 'polygon'
LEFT JOIN {{ ref('tokens_erc20') }} t_sold
    ON t_sold.contract_address = e.token_sold_address
    AND t_sold.blockchain = 'polygon'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', e.block_time)
    AND p_bought.contract_address = e.token_bought_address
    AND p_bought.blockchain = 'polygon'
    {% if not is_incremental() %}
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', e.block_time)
    AND p_sold.contract_address = e.token_sold_address
    AND p_sold.blockchain = 'polygon'
    {% if not is_incremental() %}
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
