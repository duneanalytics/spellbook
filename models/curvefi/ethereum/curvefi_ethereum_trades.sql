{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "curvefi",
                                \'["jeff-dude", "yulesa", "dsalv"]\') }}'
    )
}}


{% set project_start_date = '2017-09-27 00:00:00' %}

{% set dai_ethereum_token = "0x6b175474e89094c44da98b954eedeac495271d0f" %}
{% set usdc_ethereum_token = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" %}
{% set usdt_ethereum_token = "0xdac17f958d2ee523a2206206994597c13d831ec7" %}
{% set proxy_susd_token = "0x57ab1ec28d129707052df4df418d58a2d46d5f51" %}

{% set tusd_token = "0x0000000000085d4780b73119b644ae5ecd22b376" %}
{% set busd_token = "0x4fabb145d64652a948d72533023f6e7a623c7c53" %}

{% set ysusd_token = "0xf61718057901f84c4eec4339ef8f0d86d2b45600" %}
{% set ycrv_token = "0xdf5e0e81dff6faf3a7e52ba697820c5e32d806a8" %}

{% set cdai_token = "0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643" %}
{% set cusdc_token = "0x39aa39c021dfbae8fac545936693ac917d5e7563" %}

{% set ydai_token = "0x16de59092dae5ccf4a1e6439d611fd0653f0bd01" %}
{% set yusdc_token = "0xd6ad7a6750a7593e092a9b218d66c0a814a3436e" %}
{% set yusdt_token = "0x83f798e925bcd4017eb265844fddabb448f1707d" %}
{% set ytusd_token = "0x73a052500105205d34daf004eab301916da8190f" %}

{% set ydai_token3 = "0xc2cb1040220768554cf699b0d863a3cd4324ce32" %}
{% set yusdc_token3 = "0x26ea744e5b887e5205727f55dfbe8685e3b21951" %}
{% set yusdt_token3 = "0xe6354ed5bc4b393a5aad09f21c46e101e692d447" %}
{% set ybusd_token = "0x04bc0ab673d88ae9dbc9da2380cb6b79c4bca9ae" %}

{% set ycdai_token = "0x99d1fa417f94dcd62bfe781a1213c092a47041bc" %}
{% set ycusdc_token3 = "0x9777d7e2b60bb01759d0e2f8be2095df444cb07e" %}
{% set ycusdt_token3 = "0x1be5d71f2da660bfdee8012ddc58d024448a0a59" %}
{% set usdp_token = "0x8e870d67f660d95d5be530380d0ec0bd388289e1" %}

{% set renBTC_ethereum_token = "0xeb4c2781e4eba804ce9a9803c67d0893436bb27d" %}
{% set wbtc_ethereum_token = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599" %}
{% set sBTC_ethereum_token = "0xfe18be6b3bd88a2d2a7f928d00292e7a9963cfc6" %}

{% set weth_contract = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" %}
{% set steth_contract = "0xae7ab96520de3a18e5e111b5eaab095312d7fe84" %}

WITH dexs AS
(
    -- Curvefi TokenExchange
    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN bought_id = 2 THEN '{{usdt_ethereum_token}}'
            WHEN bought_id = 3 THEN '{{proxy_susd_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN sold_id = 2 THEN '{{usdt_ethereum_token}}'
            WHEN sold_id = 3 THEN '{{proxy_susd_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'susd_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    -- Curvefi TokenExchangeUnderlying
    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN bought_id = 2 THEN '{{usdt_ethereum_token}}'
            WHEN bought_id = 3 THEN '{{proxy_susd_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN sold_id = 2 THEN '{{usdt_ethereum_token}}'
            WHEN sold_id = 3 THEN '{{proxy_susd_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'susd_swap_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{ysusd_token}}'
            WHEN bought_id = 1 THEN '{{ycrv_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{ysusd_token}}'
            WHEN sold_id = 1 THEN '{{ycrv_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'susd_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{ysusd_token}}'
            WHEN bought_id = 1 THEN '{{ycrv_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{ysusd_token}}'
            WHEN sold_id = 1 THEN '{{ycrv_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'susd_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{cdai_token}}'
            WHEN bought_id = 1 THEN '{{cusdc_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{cdai_token}}'
            WHEN sold_id = 1 THEN '{{cusdc_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'compound_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{cdai_token}}'
            WHEN bought_id = 1 THEN '{{cusdc_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{cdai_token}}'
            WHEN sold_id = 1 THEN '{{cusdc_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'compound_v2_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{usdc_ethereum_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{usdc_ethereum_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'compound_v2_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{cdai_token}}'
            WHEN bought_id = 1 THEN '{{cusdc_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{cdai_token}}'
            WHEN sold_id = 1 THEN '{{cusdc_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'compound_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{usdc_ethereum_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{usdc_ethereum_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'compound_swap_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{cdai_token}}'
            WHEN bought_id = 1 THEN '{{cusdc_token}}'
            WHEN bought_id = 2 THEN '{{usdt_ethereum_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{cdai_token}}'
            WHEN sold_id = 1 THEN '{{cusdc_token}}'
            WHEN sold_id = 2 THEN '{{usdt_ethereum_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'usdt_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN bought_id = 2 THEN '{{usdt_ethereum_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN sold_id = 2 THEN '{{usdt_ethereum_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'usdt_swap_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{ydai_token}}'
            WHEN bought_id = 1 THEN '{{yusdc_token}}'
            WHEN bought_id = 2 THEN '{{yusdt_token}}'
            WHEN bought_id = 3 THEN '{{ytusd_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{ydai_token}}'
            WHEN sold_id = 1 THEN '{{yusdc_token}}'
            WHEN sold_id = 2 THEN '{{yusdt_token}}'
            WHEN sold_id = 3 THEN '{{ytusd_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'y_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN bought_id = 2 THEN '{{usdt_ethereum_token}}'
            WHEN bought_id = 3 THEN '{{tusd_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN sold_id = 2 THEN '{{usdt_ethereum_token}}'
            WHEN sold_id = 3 THEN '{{tusd_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'y_swap_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{ydai_token3}}'
            WHEN bought_id = 1 THEN '{{yusdc_token3}}'
            WHEN bought_id = 2 THEN '{{yusdt_token3}}'
            WHEN bought_id = 3 THEN '{{ybusd_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{ydai_token3}}'
            WHEN sold_id = 1 THEN '{{yusdc_token3}}'
            WHEN sold_id = 2 THEN '{{yusdt_token3}}'
            WHEN sold_id = 3 THEN '{{ybusd_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'busd_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN bought_id = 2 THEN '{{usdt_ethereum_token}}'
            WHEN bought_id = 3 THEN '{{busd_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN sold_id = 2 THEN '{{usdt_ethereum_token}}'
            WHEN sold_id = 3 THEN '{{busd_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'busd_swap_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{ycdai_token}}'
            WHEN bought_id = 1 THEN '{{ycusdc_token3}}'
            WHEN bought_id = 2 THEN '{{ycusdt_token3}}'
            WHEN bought_id = 3 THEN '{{usdp_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{ycdai_token}}'
            WHEN sold_id = 1 THEN '{{ycusdc_token3}}'
            WHEN sold_id = 2 THEN '{{ycusdt_token3}}'
            WHEN sold_id = 3 THEN '{{usdp_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'pax_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN bought_id = 2 THEN '{{usdt_ethereum_token}}'
            WHEN bought_id = 3 THEN '{{usdp_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN sold_id = 2 THEN '{{usdt_ethereum_token}}'
            WHEN sold_id = 3 THEN '{{usdp_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'pax_swap_evt_TokenExchangeUnderlying') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{renBTC_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{wbtc_ethereum_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{renBTC_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{wbtc_ethereum_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'ren_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{renBTC_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{wbtc_ethereum_token}}'
            WHEN bought_id = 2 THEN '{{sBTC_ethereum_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{renBTC_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{wbtc_ethereum_token}}'
            WHEN sold_id = 2 THEN '{{sBTC_ethereum_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'sbtc_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            --change address back to renBTC's, right now Dune only tracks WBTC price
            WHEN bought_id = 0 THEN '{{sBTC_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{wbtc_ethereum_token}}'
        END as token_bought_address,
        CASE
            --change address back to renBTC's, right now Dune only tracks WBTC price
            WHEN sold_id = 0 THEN '{{sBTC_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{wbtc_ethereum_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'hbtc_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN bought_id = 2 THEN '{{usdt_ethereum_token}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{dai_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{usdc_ethereum_token}}'
            WHEN sold_id = 2 THEN '{{usdt_ethereum_token}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'threepool_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{weth_contract}}'
            WHEN bought_id = 1 THEN '{{steth_contract}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{weth_contract}}'
            WHEN sold_id = 1 THEN '{{steth_contract}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'steth_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '2' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{usdt_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{wbtc_ethereum_token}}'
            WHEN bought_id = 2 THEN '{{weth_contract}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{usdt_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{wbtc_ethereum_token}}'
            WHEN sold_id = 2 THEN '{{weth_contract}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'tricrypto_swap_evt_TokenExchange') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}

    UNION

    SELECT
        evt_block_time AS block_time,
        '2' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN '{{usdt_ethereum_token}}'
            WHEN bought_id = 1 THEN '{{wbtc_ethereum_token}}'
            WHEN bought_id = 2 THEN '{{weth_contract}}'
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN '{{usdt_ethereum_token}}'
            WHEN sold_id = 1 THEN '{{wbtc_ethereum_token}}'
            WHEN sold_id = 2 THEN '{{weth_contract}}'
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
    FROM {{ source('curvefi_ethereum', 'tricrypto2_swap_evt_TokenExchange') }}   
        {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

SELECT
    'ethereum' AS blockchain
    ,'curve' AS project
    ,dexs.version AS version
    ,TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,dexs.token_bought_amount_raw
    ,dexs.token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx.from) AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx.from AS tx_from
    ,tx.to AS tx_to
    ,dexs.trace_address
    ,dexs.evt_index
FROM dexs
INNER JOIN {{ source('ethereum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange;`
    -- If dexs above is changed then this will also need to be changed.
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time = date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20a ON erc20a.contract_address = dexs.token_bought_address
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20b ON erc20b.contract_address = dexs.token_sold_address
LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'ethereum'
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange;`
    -- If dexs above is changed then this will also need to be changed.
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'ethereum'
    {% if not is_incremental() %}
    -- The date below is derrived from `select min(evt_block_time) from uniswap_ethereum.Factory_evt_NewExchange;`
    -- If dexs above is changed then this will also need to be changed.
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
