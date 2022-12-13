{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "ellipsis_finance",
                                \'["Henrystats"]\') }}'
    )
}}
-- https://www.coinbase.com/price/ellipsis
{% set project_start_date = '2021-03-01 00:00:00' %}

{%- set evt_TokenExchange_sources = [
     source('ellipsis_finance_bnb', '2brl_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', '2pool_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', '3brl_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', '3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'Ankr_BNB_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'ankr_eth_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'ankr_matic_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'apl_BUSD_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'ARTH_usd_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'AUSD_3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'axelarUSD_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'bnb_bnbl_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'BNBx_BNB_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'cryptopool_BNBx_BNB_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'cryptopool_BUSD_ARTH_bsc_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'crypto_pool_BUSD_BTCB_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'crypto_pool_BUSD_DDD_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'crypto_pool_BUSD_jCHF_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'cryptopool_dEPX_BUSD_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'crypto_pool_EPX_BNB_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'crypto_pool_ETH_BNB_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'crypto_pool_JRT_BNB_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'crypto_pool_VALAS_BNB_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'CZUSD_BUSD_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'CZUSD_val3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'deBridge_USD_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'DotDot_dEPX_EPX_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'HAY_BUSD_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'jBRL_BUSD_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'jNGN_NGNT_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'MAI_val3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'Horizon_protocol_zBNB_BNB_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'nBUSD_val3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'USDD_3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'USDL_val3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'USDN_val3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'USDS_val3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'val3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'val3EPS_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'valBTC_renBTC_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'valBTC_renBTC_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'valDAI_val3EPS_evt_TokenExchange')
    ,source('ellipsis_finance_bnb', 'valTUSD_val3EPS_evt_TokenExchange')
] -%}

{%- set evt_TokenExchangeUnderlying_sources = [
     source('ellipsis_finance_bnb', 'ARTH_usd_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'AUSD_3EPS_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'CZUSD_val3EPS_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'deBridge_USD_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'MAI_val3EPS_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'nBUSD_val3EPS_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'USDD_3EPS_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'USDL_val3EPS_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'USDN_val3EPS_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'USDS_val3EPS_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'valDAI_val3EPS_evt_TokenExchangeUnderlying')
    ,source('ellipsis_finance_bnb', 'valTUSD_val3EPS_evt_TokenExchangeUnderlying')
] -%}

WITH exchange_evt_all as (
    {%- for src in evt_TokenExchange_sources %}
    SELECT
        evt_block_time AS block_time,
        buyer AS taker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        bought_id,
        sold_id,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ src }}
        {%- if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {%- endif %}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}
),
 
exchange_und_evt_all as (
    {%- for src in evt_TokenExchangeUnderlying_sources %}
    SELECT
        evt_block_time AS block_time,
        buyer AS taker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        bought_id,
        sold_id,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        evt_index
    FROM {{ src }}
        {%- if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {%- endif %}
    {%- if not loop.last %}
    UNION ALL
    {%- endif %}
    {%- endfor %}

),

enriched_evt_all as(
    SELECT
        eb.*
        ,pa.token_address as token_bought_address
        ,pb.token_address as token_sold_address
    FROM exchange_evt_all eb
    INNER JOIN {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa
        ON eb.bought_id = pa.token_id
        AND eb.project_contract_address = pa.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    INNER JOIN
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb
        ON eb.sold_id = pb.token_id
        AND eb.project_contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    UNION ALL
    SELECT
        eb.*
        ,ut.token_address as token_bought_address
        ,COALESCE(pa.token_address, pb.token_address) as token_sold_address
    FROM exchange_und_evt_all eb
    INNER JOIN {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut
        ON eb.bought_id = ut.token_id
        AND eb.project_contract_address = ut.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.project_contract_address = pa.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.project_contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
)

SELECT
    'bnb' as blockchain,
    'ellipsis_finance' as project,
    '1' as version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) as date) as block_date,
    dexs.block_time,
    erc20a.symbol as token_bought_symbol,
    erc20b.symbol as token_sold_symbol,
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
    END as token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) as token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) as token_sold_amount,
    CAST(dexs.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw,
    COALESCE(
        (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
        (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) as amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    '' as maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx.from as tx_from,
    tx.to AS tx_to,
    '' as trace_address,
    dexs.evt_index
FROM enriched_evt_all dexs
INNER JOIN {{ source('bnb', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'bnb'
LEFT JOIN {{ ref('tokens_erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
