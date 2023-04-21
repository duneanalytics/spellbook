{{  config(
        alias='native_fills',
        materialized='incremental',
        partition_by = ['block_date'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge'
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}
{% set zeroex_v4_start_date = '2021-01-06' %}

-- Test Query here: 
WITH 
   
    v4_limit_fills AS (

        SELECT
            fills.evt_block_time AS block_time
            , 'v4' AS protocol_version
            , 'limit' as native_order_type
            , fills.evt_tx_hash AS transaction_hash
            , fills.evt_index
            , fills.maker AS maker_address
            , fills.taker AS taker_address
            , fills.makerToken AS maker_token
            , fills.takerTokenFilledAmount as taker_token_filled_amount_raw
            , fills.makerTokenFilledAmount as maker_token_filled_amount_raw
            , fills.contract_address 
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
            , fills.protocolFeePaid / 1e18 AS protocol_fee_paid_eth
        FROM {{ source('zeroex_arbitrum', 'ExchangeProxy_evt_LimitOrderFilled') }} fills
        LEFT JOIN prices.usd tp ON
            date_trunc('minute', evt_block_time) = tp.minute and  tp.blockchain = 'arbitrum'
            AND CASE
                    -- set native token to wrapped version
                    WHEN fills.takerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                    ELSE fills.takerToken
                END = tp.contract_address
        LEFT JOIN prices.usd mp ON
            DATE_TRUNC('minute', evt_block_time) = mp.minute  
            AND CASE
                    -- set native token to wrapped version
                    WHEN fills.makerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                    ELSE fills.makerToken
                END = mp.contract_address
        LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = fills.makerToken
        LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = fills.takerToken
         where 1=1  and mp.blockchain = 'arbitrum' and tp.blockchain = 'arbitrum'  
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
          , 'rfq' as native_order_type
          , fills.evt_tx_hash AS transaction_hash
          , fills.evt_index
          , fills.maker AS maker_address
          , fills.taker AS taker_address
          , fills.makerToken AS maker_token
          , fills.takerTokenFilledAmount as taker_token_filled_amount_raw
          , fills.makerTokenFilledAmount as maker_token_filled_amount_raw
          , fills.contract_address 
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
      FROM {{ source('zeroex_arbitrum', 'ExchangeProxy_evt_RfqOrderFilled') }} fills
      LEFT JOIN prices.usd tp ON
          date_trunc('minute', evt_block_time) = tp.minute 
          AND CASE
                  -- set native token to wrapped version
                    WHEN fills.takerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                    ELSE fills.takerToken
              END = tp.contract_address
      LEFT JOIN prices.usd mp ON
          DATE_TRUNC('minute', evt_block_time) = mp.minute  
          AND CASE
                  -- set native token to wrapped version
                    WHEN fills.makerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                    ELSE fills.makerToken
              END = mp.contract_address
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = fills.makerToken
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = fills.takerToken
       where 1=1  and  mp.blockchain = 'arbitrum' and tp.blockchain = 'arbitrum'
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
          , 'otc' as native_order_type
          , 'v4' AS protocol_version
          , fills.evt_tx_hash AS transaction_hash
          , fills.evt_index
          , fills.maker AS maker_address
          , fills.taker AS taker_address
          , fills.makerToken AS maker_token
          , fills.takerTokenFilledAmount as taker_token_filled_amount_raw
          , fills.makerTokenFilledAmount as maker_token_filled_amount_raw
          , fills.contract_address 
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
        FROM {{ source('zeroex_arbitrum', 'ExchangeProxy_evt_OtcOrderFilled') }} fills
      LEFT JOIN prices.usd tp ON
          date_trunc('minute', evt_block_time) = tp.minute 
          AND CASE
                  -- set native token to wrapped version
                    WHEN fills.takerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                    ELSE fills.takerToken
              END = tp.contract_address
      LEFT JOIN prices.usd mp ON
          DATE_TRUNC('minute', evt_block_time) = mp.minute  
          AND CASE
                  -- set native token to wrapped version
                    WHEN fills.makerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                    ELSE fills.makerToken
              END = mp.contract_address
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = fills.makerToken
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = fills.takerToken
       where 1=1  and mp.blockchain = 'arbitrum' and tp.blockchain = 'arbitrum'  
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}

    ),

    all_fills as (
    
   

    SELECT * FROM v4_limit_fills

    UNION ALL

    SELECT * FROM v4_rfq_fills

    UNION ALL
    
    SELECT * FROM otc_fills
    )
            SELECT distinct 
                all_fills.block_time as block_time,
                protocol_version as version,
                date_trunc('day', all_fills.block_time) as block_date,
                transaction_hash as tx_hash,
                evt_index,
                maker_address as maker,
                taker_address as taker,
                maker_token,
                maker_token_filled_amount_raw as maker_token_amount_raw,
                taker_token_filled_amount_raw as taker_token_amount_raw,
                maker_symbol,
                CASE WHEN lower(ts.symbol) > lower(ms.symbol) THEN concat(ms.symbol, '-', ts.symbol) ELSE concat(ts.symbol, '-', ms.symbol) END AS token_pair,
                CAST(ARRAY() as array<bigint>) as trace_address,
                maker_asset_filled_amount maker_token_amount,
                taker_token,
                taker_symbol,
                taker_asset_filled_amount taker_token_amount,
                fee_recipient_address,
                volume_usd,
                cast(protocol_fee_paid_eth as double),
                'arbitrum' as blockchain,
                all_fills.contract_address,
                native_order_type,
                tx.from AS tx_from,
                tx.to AS tx_to
            FROM all_fills
            INNER JOIN {{ source('arbitrum', 'transactions')}} tx ON all_fills.transaction_hash = tx.hash
            LEFT OUTER JOIN {{ ref('tokens_erc20') }} ts ON ts.contract_address = taker_token and ts.blockchain = 'arbitrum'
            LEFT OUTER JOIN {{ ref('tokens_erc20') }} ms ON ms.contract_address = maker_token and ms.blockchain = 'arbitrum'
            