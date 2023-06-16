{{ config(
    alias = 'trades'
    ,partition_by = ['block_date']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['blockchain', 'tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2021-06-01' %}
{% set zerion_native_currency_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
{% set native_wrapped_currency_address = '0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83' %}
{% set native_currency_symbol = 'FTM' %}

WITH zerion_trades AS (
    SELECT swap.evt_block_time AS block_time
    , date_trunc('day', swap.evt_block_time) AS block_date
    , swap.evt_block_number AS block_number
    , swap.sender AS trader
    , CASE WHEN swap.inputToken='{{zerion_native_currency_address}}' THEN '{{native_wrapped_currency_address}}'
        ELSE swap.inputToken
        END AS token_sold_address
    , CASE WHEN swap.inputToken='{{zerion_native_currency_address}}' THEN '{{native_currency_symbol}}'
        ELSE tok_sold.symbol
        END AS token_sold_symbol
    , swap.absoluteInputAmount AS token_sold_amount_raw
    , tok_sold.decimals AS tok_sold_decimals
    , CASE WHEN swap.outputToken='{{zerion_native_currency_address}}' THEN '{{native_wrapped_currency_address}}'
        ELSE swap.outputToken
        END AS token_bought_address
    , CASE WHEN swap.outputToken='{{zerion_native_currency_address}}' THEN '{{native_currency_symbol}}'
        ELSE tok_bought.symbol
        END AS token_bought_symbol
    , swap.absoluteOutputAmount AS token_bought_amount_raw
    , tok_bought.decimals AS tok_bought_decimals
    , pt.from AS tx_from
    , pt.to AS tx_to
    , swap.evt_tx_hash AS tx_hash
    , swap.contract_address
    , swap.evt_index
    , swap.marketplaceFeeAmount AS marketplace_fee_amount_raw
    , swap.protocolFeeAmount AS zerion_fee_amount_raw
    FROM {{ source('zerion_fantom', 'Router_evt_Executed') }} swap
    INNER JOIN {{ source('fantom','transactions') }} pt ON pt.block_number=swap.evt_block_number
        AND pt.hash=swap.evt_tx_hash
        {% if not is_incremental() %}
        AND pt.block_time >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND pt.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ ref('tokens_fantom_erc20') }} tok_sold ON tok_sold.contract_address=swap.inputToken
    LEFT JOIN {{ ref('tokens_fantom_erc20') }} tok_bought ON tok_bought.contract_address=swap.outputToken
    {% if not is_incremental() %}
    WHERE swap.evt_block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    WHERE swap.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    )

SELECT 'fantom' AS blockchain
, trades.block_time
, trades.block_date
, trades.block_number
, trades.trader
, trades.token_sold_address
, trades.token_sold_symbol
, CAST(trades.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw
, CAST(trades.token_sold_amount_raw/POWER(10, COALESCE(trades.tok_sold_decimals, pu_sold.decimals)) AS double) AS token_sold_amount_original
, trades.token_bought_address
, trades.token_bought_symbol
, CAST(trades.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw
, CAST(trades.token_bought_amount_raw/POWER(10, COALESCE(trades.tok_bought_decimals, pu_bought.decimals)) AS double) AS token_bought_amount_original
, CAST(COALESCE(trades.token_sold_amount_raw/POWER(10, COALESCE(trades.tok_sold_decimals, pu_sold.decimals))
    , trades.token_bought_amount_raw/POWER(10, COALESCE(trades.tok_bought_decimals, pu_bought.decimals))) AS double) AS amount_usd
, trades.tx_from
, trades.tx_to
, trades.tx_hash
, trades.contract_address
, trades.evt_index
, CAST(trades.marketplace_fee_amount_raw AS DECIMAL(38,0)) AS marketplace_fee_amount_raw
, CAST(CASE WHEN trades.marketplace_fee_amount_raw= 0 THEN 0
    ELSE CAST(trades.marketplace_fee_amount_raw/POWER(10, COALESCE(trades.tok_bought_decimals, pu_bought.decimals)) AS double)
    END AS double) AS marketplace_fee_amount_original
, CAST(trades.zerion_fee_amount_raw AS DECIMAL(38,0)) AS zerion_fee_amount_raw
, CAST(CASE WHEN trades.zerion_fee_amount_raw= 0 THEN 0
    ELSE CAST(trades.zerion_fee_amount_raw/POWER(10, COALESCE(trades.tok_sold_decimals, pu_sold.decimals)) AS double)
    END AS double) AS zerion_fee_amount_original
FROM zerion_trades trades
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu_sold ON pu_sold.blockchain='fantom'
    AND pu_sold.contract_address=trades.token_sold_address
    AND pu_sold.minute=date_trunc('minute', trades.block_time)
    {% if not is_incremental() %}
    AND pu_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND pu_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu_bought ON pu_bought.blockchain='fantom'
    AND pu_bought.contract_address=trades.token_bought_address
    AND pu_bought.minute=date_trunc('minute', trades.block_time)
    {% if not is_incremental() %}
    AND pu_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND pu_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}