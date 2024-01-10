{{ config(
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "onepunchswap",
                                \'["sunopar"]\') }}'
    )
}}

{% set project_start_date = '2023-01-01' %}
{% set onepunchswap_bnb_evt_trade_tables = [
    source('onepunch_normal_bnb', 'DexLiquidityProvider_evt_QuoteAccepted')
    , source('onepunch_quick_bnb', 'QuickLiquidityProvider_evt_QuoteAccepted')
] %}

WITH dexs AS(
  {% for evt_trade_table in onepunchswap_bnb_evt_trade_tables %}
    SELECT
        evt_block_time                AS block_time,
        user                          AS taker,
        CAST(NULL AS VARBINARY)                            AS maker,
        cast(json_extract_scalar(quoteInfo,'$.toAmount') as UINT256) as token_bought_amount_raw,
        cast(json_extract_scalar(quoteInfo,'$.fromAmount') as UINT256) as token_sold_amount_raw,
        NULL          AS amount_usd,
        case 
            when json_extract_scalar(quoteInfo,'$.toAsset') = '0x0000000000000000000000000000000000000000' then 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
            else from_hex(json_extract_scalar(quoteInfo,'$.toAsset'))
        end                                                 as token_bought_address,
        case 
            when json_extract_scalar(quoteInfo,'$.fromAsset') = '0x0000000000000000000000000000000000000000' then 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c
            else from_hex(json_extract_scalar(quoteInfo,'$.fromAsset'))
        end                                                 as token_sold_address,

        contract_address              AS project_contract_address,
        evt_tx_hash                   AS tx_hash,
        evt_index
      FROM
        {{ evt_trade_table }}
        {% if not is_incremental() %}
        WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        {% if not loop.last %}
        UNION ALL
        {% endif %}

    {% endfor %}
)

SELECT 'bnb'                                                     AS blockchain
     , 'onepunchswap'                                            AS project
     , case
           when dexs.project_contract_address = 0xeeb28c597dc67ed4a337c14b20b0a5c353e38253 then 'quick'
           else 'normal'
    end                                                          AS version
     , TRY_CAST(date_trunc('DAY', dexs.block_time) AS date)      AS block_date
     , CAST(date_trunc('month', dexs.block_time) AS date)      AS block_month
     , dexs.block_time
     , bep20a.symbol                                             AS token_bought_symbol
     , bep20b.symbol                                             AS token_sold_symbol
     , dexs.token_bought_address                                 AS token_bought_address
     , dexs.token_sold_address                                   AS token_sold_address
     , case
           when lower(bep20a.symbol) > lower(bep20b.symbol) then concat(bep20b.symbol, '-', bep20a.symbol)
           else concat(bep20a.symbol, '-', bep20b.symbol)
    end                                                          AS token_pair
     , dexs.token_bought_amount_raw / power(10, bep20a.decimals) AS token_bought_amount
     , dexs.token_sold_amount_raw / power(10, bep20b.decimals)   AS token_sold_amount
     , dexs.token_bought_amount_raw       AS token_bought_amount_raw
     , dexs.token_sold_amount_raw         AS token_sold_amount_raw
     , coalesce(
        dexs.amount_usd
    , (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
    )                                                            AS amount_usd
     , dexs.maker
     , dexs.taker
     , dexs.project_contract_address
     , dexs.tx_hash
     , tx."from"                                                   AS tx_from
     , tx.to                                                     AS tx_to
     , dexs.evt_index
FROM dexs
INNER JOIN {{ source('bnb', 'transactions') }} tx
    ON dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} bep20a
    ON bep20a.contract_address = dexs.token_bought_address
    AND bep20a.blockchain = 'bnb'
LEFT JOIN {{ source('tokens', 'erc20') }} bep20b
    ON bep20b.contract_address = dexs.token_sold_address
    AND bep20b.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
