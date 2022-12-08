{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "ellipsis_finance",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2021-03-01 00:00:00' %} -- https://www.coinbase.com/price/ellipsis

WITH 

2brl as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', '2brl_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

2pool as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', '2pool_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

3brl as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', '3brl_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', '3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

ankr_bnb as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'Ankr_BNB_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

ankr_eth as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'ankr_eth_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

ankr_matic as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'ankr_matic_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

apl_busd as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'apl_BUSD_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

arth_usd as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'ARTH_usd_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

arth_usd_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'ARTH_usd_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

ausd_3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'AUSD_3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

ausd_3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'ARTH_usd_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

axelar_usd as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'axelarUSD_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

bnb_bnbl as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'bnb_bnbl_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

bnbx_bnb as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'BNBx_BNB_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

cryptopool_bnbx_bnb as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'cryptopool_BNBx_BNB_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

cryptopool_busd_arth as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'cryptopool_BUSD_ARTH_bsc_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

cryptopool_busd_btcb as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'crypto_pool_BUSD_BTCB_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

cryptopool_busd_ddd as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'crypto_pool_BUSD_DDD_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

cryptopool_busd_jchf as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'crypto_pool_BUSD_jCHF_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

cryptopool_depx_busd as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'cryptopool_dEPX_BUSD_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

cryptopool_epx_bnb as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'crypto_pool_EPX_BNB_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

cryptopool_eth_bnb as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'crypto_pool_ETH_BNB_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

cryptopool_jrt_bnb as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'crypto_pool_JRT_BNB_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

cryptopool_valas_bnb as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'crypto_pool_VALAS_BNB_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

czusd_busd as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'CZUSD_BUSD_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

czusd_val3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'CZUSD_val3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

czusd_val3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'CZUSD_val3EPS_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

debridge_usd as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'deBridge_USD_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

debridge_usd_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'deBridge_USD_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

dotdot_depx_epx as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'DotDot_dEPX_EPX_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

hay_busd as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'HAY_BUSD_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

horizon_zbnb_bnb as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'Horizon_protocol_zBNB_BNB_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

jbrl_busd as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'jBRL_BUSD_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

jngn_ngnt as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'jNGN_NGNT_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

mai_val3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'MAI_val3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

mai_val3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'MAI_val3EPS_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

nbusd_val3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'nBUSD_val3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

nbusd_val3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'nBUSD_val3EPS_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

hay_stableswap as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'StableSwap_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

usdd_3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'USDD_3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

usdd_3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'USDD_3EPS_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

usdl_val3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'USDL_val3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

usdl_val3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'USDL_val3EPS_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

usdn_val3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'USDN_val3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

usdn_val3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'USDN_val3EPS_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

usds_val3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'USDS_val3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

usds_val3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'USDS_val3EPS_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

val3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'val3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

val3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'val3EPS_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

valbtc_renbtc as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'valBTC_renBTC_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

valbtc_renbtc_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'valBTC_renBTC_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

valdai_val3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'valDAI_val3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

valdai_val3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'valDAI_val3EPS_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

valtusd_val3eps as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        pa.token_address as token_bought_address, 
        pb.token_address as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'valTUSD_val3EPS_evt_TokenExchange') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.bought_id = pa.token_id
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON eb.sold_id = pb.token_id
        AND eb.contract_address = pb.pool
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
), 

valtusd_val3eps_underlying as (
    SELECT 
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        CAST(NULL as double) as amount_usd,
        ut.token_address as token_bought_address, 
        COALESCE(pa.token_address, pb.token_address) as token_sold_address, 
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('ellipsis_finance_bnb', 'valTUSD_val3EPS_evt_TokenExchangeUnderlying') }} eb 
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_underlying_tokens') }} ut 
        ON eb.bought_id = ut.token_id
        AND eb.contract_address = ut.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pa 
        ON eb.sold_id = pa.token_id
        AND eb.sold_id = '0'
        AND eb.contract_address = pa.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN 
    {{ ref('ellipsis_finance_bnb_pool_tokens') }} pb 
        ON pb.token_id = '1'
        AND eb.sold_id  != '0'
        AND eb.contract_address = pb.pool 
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
),

dexs as (
    SELECT * FROM 2brl 

    UNION 

    SELECT * FROM 2pool 

    UNION 

    SELECT * FROM 3brl 

    UNION 

    SELECT * FROM 3eps 

    UNION 

    SELECT * FROM ankr_bnb

    UNION 

    SELECT * FROM ankr_eth

    UNION 

    SELECT * FROM ankr_matic

    UNION 

    SELECT * FROM apl_busd

    UNION 

    SELECT * FROM arth_usd

    UNION 

    SELECT * FROM arth_usd_underlying

    UNION 

    SELECT * FROM ausd_3eps

    UNION 

    SELECT * FROM ausd_3eps_underlying

    UNION 

    SELECT * FROM axelar_usd

    UNION 

    SELECT * FROM bnb_bnbl

    UNION 

    SELECT * FROM bnbx_bnb

    UNION 

    SELECT * FROM cryptopool_bnbx_bnb

    UNION 

    SELECT * FROM cryptopool_busd_arth

    UNION 

    SELECT * FROM cryptopool_busd_btcb

    UNION 

    SELECT * FROM cryptopool_busd_ddd

    UNION 

    SELECT * FROM cryptopool_busd_jchf

    UNION 

    SELECT * FROM cryptopool_depx_busd

    UNION 

    SELECT * FROM cryptopool_epx_bnb

    UNION 

    SELECT * FROM cryptopool_eth_bnb

    UNION 

    SELECT * FROM cryptopool_jrt_bnb

    UNION 

    SELECT * FROM cryptopool_valas_bnb

    UNION 

    SELECT * FROM czusd_busd

    UNION 

    SELECT * FROM czusd_val3eps

    UNION 

    SELECT * FROM czusd_val3eps_underlying

    UNION 

    SELECT * FROM debridge_usd

    UNION 

    SELECT * FROM debridge_usd_underlying

    UNION 

    SELECT * FROM dotdot_depx_epx

    UNION 

    SELECT * FROM hay_busd

    UNION 

    SELECT * FROM horizon_zbnb_bnb

    UNION 

    SELECT * FROM jbrl_busd

    UNION 

    SELECT * FROM jngn_ngnt

    UNION 

    SELECT * FROM mai_val3eps

    UNION 

    SELECT * FROM mai_val3eps_underlying

    UNION 

    SELECT * FROM nbusd_val3eps

    UNION 

    SELECT * FROM nbusd_val3eps_underlying

    UNION 

    SELECT * FROM hay_stableswap

    UNION 

    SELECT * FROM usdd_3eps

    UNION 

    SELECT * FROM usdd_3eps_underlying

    UNION 

    SELECT * FROM usdl_val3eps

    UNION 

    SELECT * FROM usdl_val3eps_underlying

    UNION 

    SELECT * FROM usdn_val3eps

    UNION 

    SELECT * FROM usdn_val3eps_underlying

    UNION 

    SELECT * FROM usds_val3eps

    UNION 

    SELECT * FROM usds_val3eps_underlying

    UNION 

    SELECT * FROM val3eps

    UNION 

    SELECT * FROM val3eps_underlying

    UNION 

    SELECT * FROM valbtc_renbtc

    UNION 

    SELECT * FROM valbtc_renbtc_underlying

    UNION 

    SELECT * FROM valdai_val3eps

    UNION 

    SELECT * FROM valdai_val3eps_underlying

    UNION 

    SELECT * FROM valtusd_val3eps

    UNION 

    SELECT * FROM valtusd_val3eps_underlying
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
        dexs.amount_usd, 
        (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price, 
        (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) as amount_usd, 
    dexs.token_bought_address, 
    dexs.token_sold_address, 
    COALESCE(dexs.taker, tx.from) as taker,  -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    dexs.maker, 
    dexs.project_contract_address, 
    dexs.tx_hash, 
    tx.from as tx_from, 
    tx.to AS tx_to, 
    dexs.trace_address, 
    dexs.evt_index
FROM dexs
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