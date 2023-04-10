{{  config(
        alias='fills',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "zeroex",
                                \'["danning.sui", "bakabhai993", "rantum"]\') }}'
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}

-- Test Query here: 
WITH 
    v3_fills AS (
        SELECT
            evt_block_time AS block_time
            , 'v3' AS protocol_version
            , fills.evt_tx_hash AS transaction_hash
            , fills.evt_index
            , fills.makerAddress AS maker_address
            , fills.takerAddress AS taker_address
            , SUBSTRING(fills.makerAssetData,17,20) AS maker_token
            , mt.symbol AS maker_symbol
            , fills.takerAssetFilledAmount as taker_token_filled_amount_raw
            , fills.makerAssetFilledAmount as maker_token_filled_amount_raw
            , fills.makerAssetFilledAmount / (10^mt.decimals) AS maker_asset_filled_amount
            , SUBSTRING(fills.takerAssetData,17,20) AS taker_token
            , tt.symbol AS taker_symbol
            , fills.takerAssetFilledAmount / (10^tt.decimals) AS taker_asset_filled_amount
            , fills.feeRecipientAddress AS fee_recipient_address
            , CASE
                    WHEN tp.symbol = 'USDC' THEN (fills.takerAssetFilledAmount / 1e6) --don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'USDC' THEN (fills.makerAssetFilledAmount / 1e6) --don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'TUSD' THEN (fills.takerAssetFilledAmount / 1e18) --don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'TUSD' THEN (fills.makerAssetFilledAmount / 1e18) --don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'USDT' THEN (fills.takerAssetFilledAmount / 1e6) * tp.price
                    WHEN mp.symbol = 'USDT' THEN (fills.makerAssetFilledAmount / 1e6) * mp.price
                    WHEN tp.symbol = 'DAI' THEN (fills.takerAssetFilledAmount / 1e18) * tp.price
                    WHEN mp.symbol = 'DAI' THEN (fills.makerAssetFilledAmount / 1e18) * mp.price
                    WHEN tp.symbol = 'WETH' THEN (fills.takerAssetFilledAmount / 1e18) * tp.price
                    WHEN mp.symbol = 'WETH' THEN (fills.makerAssetFilledAmount / 1e18) * mp.price
                    ELSE COALESCE((fills.makerAssetFilledAmount / (10^mt.decimals))*mp.price,(fills.takerAssetFilledAmount / (10^tt.decimals))*tp.price)
                END AS volume_usd
            , fills.protocolFeePaid / 1e18 AS protocol_fee_paid_eth,
            fills.contract_address
            , 'fills' as native_order_type
        FROM {{ source('zeroex_v3_ethereum', 'Exchange_evt_Fill') }} fills 
        LEFT JOIN prices.usd tp ON
            date_trunc('minute', evt_block_time) = tp.minute 
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills.takerAssetData,17,20) IN ('0x50cb61afa3f023d17276dcfb35abf85c710d1cff','0xaa7427d8f17d87a28f5e1ba3adbb270badbe1011') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN SUBSTRING(fills.takerAssetData,17,20) IN ('0x69391cca2e38b845720c7deb694ec837877a8e53') THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                    ELSE SUBSTRING(fills.takerAssetData,17,20)
                END = tp.contract_address
        LEFT JOIN prices.usd mp ON
            DATE_TRUNC('minute', evt_block_time) = mp.minute  
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills.makerAssetData,17,20) IN ('0x50cb61afa3f023d17276dcfb35abf85c710d1cff','0xaa7427d8f17d87a28f5e1ba3adbb270badbe1011') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN SUBSTRING(fills.makerAssetData,17,20) IN ('0x69391cca2e38b845720c7deb694ec837877a8e53') THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                    ELSE SUBSTRING(fills.makerAssetData,17,20)
                END = mp.contract_address
        LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = SUBSTRING(fills.makerAssetData,17,20)
        LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = SUBSTRING(fills.takerAssetData,17,20)
         where 1=1  and mp.blockchain = 'ethereum' and tp.blockchain = 'ethereum'
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}
    )
    , v2_1_fills AS (
        SELECT
            evt_block_time AS block_time
            , 'v2' AS protocol_version
            , fills.evt_tx_hash AS transaction_hash
            , fills.evt_index
            , fills.makerAddress AS maker_address
            , fills.takerAddress AS taker_address
            , SUBSTRING(fills.makerAssetData,17,20) AS maker_token
            , fills.takerAssetFilledAmount as taker_token_filled_amount_raw
            , fills.makerAssetFilledAmount as maker_token_filled_amount_raw
            , mt.symbol AS maker_symbol
            , fills.makerAssetFilledAmount / (10^mt.decimals) AS maker_asset_filled_amount
            , SUBSTRING(fills.takerAssetData,17,20) AS taker_token
            , tt.symbol AS taker_symbol
            , fills.takerAssetFilledAmount / (10^tt.decimals) AS taker_asset_filled_amount
            , fills.feeRecipientAddress AS fee_recipient_address
            , CASE
                    WHEN tp.symbol = 'USDC' THEN (fills.takerAssetFilledAmount / 1e6) ----don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'USDC' THEN (fills.makerAssetFilledAmount / 1e6) ----don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'TUSD' THEN (fills.takerAssetFilledAmount / 1e18) --don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'TUSD' THEN (fills.makerAssetFilledAmount / 1e18) --don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'USDT' THEN (fills.takerAssetFilledAmount / 1e6) * tp.price
                    WHEN mp.symbol = 'USDT' THEN (fills.makerAssetFilledAmount / 1e6) * mp.price
                    WHEN tp.symbol = 'DAI' THEN (fills.takerAssetFilledAmount / 1e18) * tp.price
                    WHEN mp.symbol = 'DAI' THEN (fills.makerAssetFilledAmount / 1e18) * mp.price
                    WHEN tp.symbol = 'WETH' THEN (fills.takerAssetFilledAmount / 1e18) * tp.price
                    WHEN mp.symbol = 'WETH' THEN (fills.makerAssetFilledAmount / 1e18) * mp.price
                    ELSE COALESCE((fills.makerAssetFilledAmount / (10^mt.decimals))*mp.price,(fills.takerAssetFilledAmount / (10^tt.decimals))*tp.price)
                END AS volume_usd, fills.contract_address
            , NULL::NUMERIC AS protocol_fee_paid_eth
            , 'fills' as native_order_type
        FROM {{ source('zeroex_v2_ethereum', 'Exchange2_1_evt_Fill') }} fills
        LEFT JOIN prices.usd tp ON
            date_trunc('minute', evt_block_time) = tp.minute 
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills.takerAssetData,17,20) IN ('0x50cb61afa3f023d17276dcfb35abf85c710d1cff','0xaa7427d8f17d87a28f5e1ba3adbb270badbe1011') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN SUBSTRING(fills.takerAssetData,17,20) IN ('0x69391cca2e38b845720c7deb694ec837877a8e53') THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                    ELSE SUBSTRING(fills.takerAssetData,17,20)
                END = tp.contract_address
        LEFT JOIN prices.usd mp ON
            DATE_TRUNC('minute', evt_block_time) = mp.minute 
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills.makerAssetData,17,20) IN ('0x50cb61afa3f023d17276dcfb35abf85c710d1cff','0xaa7427d8f17d87a28f5e1ba3adbb270badbe1011') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN SUBSTRING(fills.makerAssetData,17,20) IN ('0x69391cca2e38b845720c7deb694ec837877a8e53') THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                    ELSE SUBSTRING(fills.makerAssetData,17,20)
                END = mp.contract_address
        LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = SUBSTRING(fills.makerAssetData,17,20)
        LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = SUBSTRING(fills.takerAssetData,17,20)
         where 1=1  and mp.blockchain = 'ethereum' and tp.blockchain = 'ethereum'
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}
    )
    , v4_limit_fills AS (

        SELECT
            fills.evt_block_time AS block_time
            , 'v4' AS protocol_version
            , fills.evt_tx_hash AS transaction_hash
            , fills.evt_index
            , fills.maker AS maker_address
            , fills.taker AS taker_address
            , fills.makerToken AS maker_token
            , fills.takerTokenFilledAmount as taker_token_filled_amount_raw
            , fills.makerTokenFilledAmount as maker_token_filled_amount_raw
            , mt.symbol AS maker_symbol
            , fills.makerTokenFilledAmount / (10^mt.decimals) AS maker_asset_filled_amount
            , fills.takerToken AS taker_token
            , tt.symbol AS taker_symbol
            , fills.takerTokenFilledAmount / (10^tt.decimals) AS taker_asset_filled_amount
            , fills.feeRecipient AS fee_recipient_address
            , CASE
                    WHEN tp.symbol = 'USDC' THEN (fills.takerTokenFilledAmount / 1e6) ----don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'USDC' THEN (fills.makerTokenFilledAmount / 1e6) ----don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'TUSD' THEN (fills.takerTokenFilledAmount / 1e18) --don't multiply by anything as these assets are USD
                    WHEN mp.symbol = 'TUSD' THEN (fills.makerTokenFilledAmount / 1e18) --don't multiply by anything as these assets are USD
                    WHEN tp.symbol = 'USDT' THEN (fills.takerTokenFilledAmount / 1e6) * tp.price
                    WHEN mp.symbol = 'USDT' THEN (fills.makerTokenFilledAmount / 1e6) * mp.price
                    WHEN tp.symbol = 'DAI' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                    WHEN mp.symbol = 'DAI' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                    WHEN tp.symbol = 'WETH' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                    WHEN mp.symbol = 'WETH' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                    ELSE COALESCE((fills.makerTokenFilledAmount / (10^mt.decimals))*mp.price,(fills.takerTokenFilledAmount / (10^tt.decimals))*tp.price)
                END AS volume_usd
            , fills.protocolFeePaid/ 1e18 AS protocol_fee_paid_eth
            , fills.contract_address
            , 'limit' as native_order_type
        FROM {{ source('zeroex_ethereum', 'ExchangeProxy_evt_LimitOrderFilled') }} fills
        LEFT JOIN prices.usd tp ON 
            date_trunc('minute', evt_block_time) = tp.minute 
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN fills.takerToken IN ('0x50cb61afa3f023d17276dcfb35abf85c710d1cff','0xaa7427d8f17d87a28f5e1ba3adbb270badbe1011') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN fills.takerToken IN ('0x69391cca2e38b845720c7deb694ec837877a8e53') THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                    ELSE fills.takerToken
                END = tp.contract_address
        LEFT JOIN prices.usd mp ON 
            DATE_TRUNC('minute', evt_block_time) = mp.minute 
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN fills.makerToken IN ('0x50cb61afa3f023d17276dcfb35abf85c710d1cff','0xaa7427d8f17d87a28f5e1ba3adbb270badbe1011') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    -- Set Deversifi USDCWrapper to USDC
                    WHEN fills.makerToken IN ('0x69391cca2e38b845720c7deb694ec837877a8e53') THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                    ELSE fills.makerToken
                END = mp.contract_address
        LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = fills.makerToken
        LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = fills.takerToken
         where 1=1  and mp.blockchain = 'ethereum' and tp.blockchain = 'ethereum'
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}
    )

    , v4_rfq_fills AS (
      SELECT
          fills.evt_block_time AS block_time
          , 'v4' AS protocol_version
          , fills.evt_tx_hash AS transaction_hash
          , fills.evt_index
          , fills.maker AS maker_address
          , fills.taker AS taker_address
          , fills.makerToken AS maker_token
          , fills.takerTokenFilledAmount as taker_token_filled_amount_raw
          , fills.makerTokenFilledAmount as maker_token_filled_amount_raw
          , mt.symbol AS maker_symbol
          , fills.makerTokenFilledAmount / (10^mt.decimals) AS maker_asset_filled_amount
          , fills.takerToken AS taker_token
          , tt.symbol AS taker_symbol
          , fills.takerTokenFilledAmount / (10^tt.decimals) AS taker_asset_filled_amount
          , NULL: AS fee_recipient_address
          , CASE
                  WHEN tp.symbol = 'USDC' THEN (fills.takerTokenFilledAmount / 1e6) ----don't multiply by anything as these assets are USD
                  WHEN mp.symbol = 'USDC' THEN (fills.makerTokenFilledAmount / 1e6) ----don't multiply by anything as these assets are USD
                  WHEN tp.symbol = 'TUSD' THEN (fills.takerTokenFilledAmount / 1e18) --don't multiply by anything as these assets are USD
                  WHEN mp.symbol = 'TUSD' THEN (fills.makerTokenFilledAmount / 1e18) --don't multiply by anything as these assets are USD
                  WHEN tp.symbol = 'USDT' THEN (fills.takerTokenFilledAmount / 1e6) * tp.price
                  WHEN mp.symbol = 'USDT' THEN (fills.makerTokenFilledAmount / 1e6) * mp.price
                  WHEN tp.symbol = 'DAI' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                  WHEN mp.symbol = 'DAI' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                  WHEN tp.symbol = 'WETH' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                  WHEN mp.symbol = 'WETH' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                  ELSE COALESCE((fills.makerTokenFilledAmount / (10^mt.decimals))*mp.price,(fills.takerTokenFilledAmount / (10^tt.decimals))*tp.price)
              END AS volume_usd
          , NULL::NUMERIC AS protocol_fee_paid_eth,
          fills.contract_address
          , 'rfq' as native_order_type
      FROM {{ source('zeroex_ethereum', 'ExchangeProxy_evt_RfqOrderFilled') }} fills
      LEFT JOIN prices.usd tp ON
          date_trunc('minute', evt_block_time) = tp.minute 
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                  WHEN fills.takerToken IN ('0x50cb61afa3f023d17276dcfb35abf85c710d1cff','0xaa7427d8f17d87a28f5e1ba3adbb270badbe1011') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                  -- Set Deversifi USDCWrapper to USDC
                  WHEN fills.takerToken IN ('0x69391cca2e38b845720c7deb694ec837877a8e53') THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                  ELSE fills.takerToken
              END = tp.contract_address
      LEFT JOIN prices.usd mp ON
          DATE_TRUNC('minute', evt_block_time) = mp.minute 
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                  WHEN fills.makerToken IN ('0x50cb61afa3f023d17276dcfb35abf85c710d1cff','0xaa7427d8f17d87a28f5e1ba3adbb270badbe1011') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                  -- Set Deversifi USDCWrapper to USDC
                  WHEN fills.makerToken IN ('0x69391cca2e38b845720c7deb694ec837877a8e53') THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                  ELSE fills.makerToken
              END = mp.contract_address
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = fills.makerToken
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = fills.takerToken
       where 1=1  and mp.blockchain = 'ethereum' and tp.blockchain = 'ethereum'
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}
    ), otc_fills as
    (
      SELECT
          fills.evt_block_time AS block_time
          , 'v4' AS protocol_version
          , fills.evt_tx_hash AS transaction_hash
          , fills.evt_index
          , fills.maker AS maker_address
          , fills.taker AS taker_address
          , fills.makerToken AS maker_token
          , fills.takerTokenFilledAmount as taker_token_filled_amount_raw
          , fills.makerTokenFilledAmount as maker_token_filled_amount_raw
          , mt.symbol AS maker_symbol
          , fills.makerTokenFilledAmount / (10^mt.decimals) AS maker_asset_filled_amount
          , fills.takerToken AS taker_token
          , tt.symbol AS taker_symbol
          , fills.takerTokenFilledAmount / (10^tt.decimals) AS taker_asset_filled_amount
          , NULL: AS fee_recipient_address
          , CASE
                  WHEN tp.symbol = 'USDC' THEN (fills.takerTokenFilledAmount / 1e6) ----don't multiply by anything as these assets are USD
                  WHEN mp.symbol = 'USDC' THEN (fills.makerTokenFilledAmount / 1e6) ----don't multiply by anything as these assets are USD
                  WHEN tp.symbol = 'TUSD' THEN (fills.takerTokenFilledAmount / 1e18) --don't multiply by anything as these assets are USD
                  WHEN mp.symbol = 'TUSD' THEN (fills.makerTokenFilledAmount / 1e18) --don't multiply by anything as these assets are USD
                  WHEN tp.symbol = 'USDT' THEN (fills.takerTokenFilledAmount / 1e6) * tp.price
                  WHEN mp.symbol = 'USDT' THEN (fills.makerTokenFilledAmount / 1e6) * mp.price
                  WHEN tp.symbol = 'DAI' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                  WHEN mp.symbol = 'DAI' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                  WHEN tp.symbol = 'WETH' THEN (fills.takerTokenFilledAmount / 1e18) * tp.price
                  WHEN mp.symbol = 'WETH' THEN (fills.makerTokenFilledAmount / 1e18) * mp.price
                  ELSE COALESCE((fills.makerTokenFilledAmount / (10^mt.decimals))*mp.price,(fills.takerTokenFilledAmount / (10^tt.decimals))*tp.price)
              END AS volume_usd
          , NULL::NUMERIC AS protocol_fee_paid_eth
          , fills.contract_address
          , 'otc' as native_order_type
      FROM {{ source('zeroex_ethereum', 'ExchangeProxy_evt_OtcOrderFilled') }} fills
      LEFT JOIN prices.usd tp ON
          date_trunc('minute', evt_block_time) = tp.minute 
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                  WHEN fills.takerToken IN ('0x50cb61afa3f023d17276dcfb35abf85c710d1cff','0xaa7427d8f17d87a28f5e1ba3adbb270badbe1011') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                  -- Set Deversifi USDCWrapper to USDC
                  WHEN fills.takerToken IN ('0x69391cca2e38b845720c7deb694ec837877a8e53') THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                  ELSE fills.takerToken
              END = tp.contract_address
      LEFT JOIN prices.usd mp ON
          DATE_TRUNC('minute', evt_block_time) = mp.minute  
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                  WHEN fills.makerToken IN ('0x50cb61afa3f023d17276dcfb35abf85c710d1cff','0xaa7427d8f17d87a28f5e1ba3adbb270badbe1011') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                  -- Set Deversifi USDCWrapper to USDC
                  WHEN fills.makerToken IN ('0x69391cca2e38b845720c7deb694ec837877a8e53') THEN '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                  ELSE fills.makerToken
              END = mp.contract_address
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = fills.makerToken
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = fills.takerToken
       where 1=1  and mp.blockchain = 'ethereum' and tp.blockchain = 'ethereum'
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}

    ),

    all_fills as (
    
    SELECT * FROM v3_fills

    UNION ALL

    SELECT * FROM v2_1_fills

    UNION ALL

    SELECT * FROM v4_limit_fills

    UNION ALL

    SELECT * FROM v4_rfq_fills

    UNION ALL
    
    SELECT * FROM otc_fills
    )
            SELECT distinct 
                all_fills.block_time as block_time,
                date_trunc('day', all_fills.block_time) as block_date,
                protocol_version as version,
                transaction_hash as tx_hash,
                evt_index,
                maker_address as maker,
                taker_address as taker,
                maker_token,
                maker_token_filled_amount_raw as maker_token_amount_raw,
                taker_token_filled_amount_raw as taker_token_amount_raw,
                maker_symbol,
                maker_asset_filled_amount maker_token_amount,
                taker_token, 
                taker_symbol,
                taker_asset_filled_amount taker_token_amount,
                fee_recipient_address,
                volume_usd as amount_usd,
                protocol_fee_paid_eth,
                'polygon' as blockchain,
                contract_address,
                native_order_type,
                tx.from AS tx_from,
                tx.to AS tx_to
            FROM all_fills
            INNER JOIN {{ source('ethereum', 'transactions')}} tx ON all_fills.transaction_hash = tx.hash