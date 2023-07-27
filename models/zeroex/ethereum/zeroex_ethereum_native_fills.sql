{{  config(
	tags=['legacy'],
	
        alias = alias('native_fills', legacy_model=True),
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
   /* v3_fills AS (
        SELECT
            evt_block_time AS block_time, fills.evt_block_number as block_number
            , 'v3' AS protocol_version
            , fills.evt_tx_hash AS transaction_hash
            , fills.evt_index
            , fills.makerAddress AS maker_address
            , fills.takerAddress AS taker_address
            , SUBSTRING(fills.makerAssetData,17,20) AS maker_token
            , mt.symbol AS maker_symbol
            , CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair
            , fills.takerAssetFilledAmount as taker_token_filled_amount_raw
            , fills.makerAssetFilledAmount as maker_token_filled_amount_raw
            , fills.makerAssetFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount
            , SUBSTRING(fills.takerAssetData,17,20) AS taker_token
            , tt.symbol AS taker_symbol
            , fills.takerAssetFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount
            , (fills.feeRecipientAddress in 
                ('0x9b858be6e3047d88820f439b240deac2418a2551','0x86003b044f70dac0abc80ac8957305b6370893ed','0x5bc2419a087666148bfbe1361ae6c06d240c6131')) 
                AS matcha_limit_order_flag
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
                  ELSE COALESCE((fills.makerAssetFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerAssetFilledAmount / pow(10, tt.decimals))*tp.price)
                END AS volume_usd
            , fills.protocolFeePaid / 1e18 AS protocol_fee_paid_eth,
            fills.contract_address
            , 'fills' as native_order_type
        FROM {{ source('zeroex_v3_ethereum', 'Exchange_evt_Fill') }} fills 
        LEFT JOIN {{ source('prices', 'usd') }} tp ON
            date_trunc('minute', evt_block_time) = tp.minute and tp.blockchain = 'ethereum'
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills.takerAssetData,17,20) IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    ELSE SUBSTRING(fills.takerAssetData,17,20)
                END = tp.contract_address
        LEFT JOIN {{ source('prices', 'usd') }} mp ON
            DATE_TRUNC('minute', evt_block_time) = mp.minute  and mp.blockchain = 'ethereum'
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills.makerAssetData,17,20) IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    ELSE SUBSTRING(fills.makerAssetData,17,20)
                END = mp.contract_address
        LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} mt ON mt.contract_address = SUBSTRING(fills.makerAssetData,17,20)
        LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} tt ON tt.contract_address = SUBSTRING(fills.takerAssetData,17,20)
         where 1=1  
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}
    )
    , v2_1_fills AS (
        SELECT
            evt_block_time AS block_time, fills.evt_block_number as block_number
            , 'v2' AS protocol_version
            , fills.evt_tx_hash AS transaction_hash
            , fills.evt_index
            , fills.makerAddress AS maker_address
            , fills.takerAddress AS taker_address
            , SUBSTRING(fills.makerAssetData,17,20) AS maker_token
            , mt.symbol AS maker_symbol
            , CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair
            , fills.takerAssetFilledAmount as taker_token_filled_amount_raw
            , fills.makerAssetFilledAmount as maker_token_filled_amount_raw
            , fills.makerAssetFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount
            , SUBSTRING(fills.takerAssetData,17,20) AS taker_token
            , tt.symbol AS taker_symbol
            , fills.takerAssetFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount
            , (fills.feeRecipientAddress in 
                ('0x9b858be6e3047d88820f439b240deac2418a2551','0x86003b044f70dac0abc80ac8957305b6370893ed','0x5bc2419a087666148bfbe1361ae6c06d240c6131')) 
                AS matcha_limit_order_flag
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
                  ELSE COALESCE((fills.makerAssetFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerAssetFilledAmount / pow(10, tt.decimals))*tp.price)
                END AS volume_usd, fills.contract_address
            , cast(null as numeric) as protocol_fee_paid_eth
            , 'fills' as native_order_type
        FROM {{ source('zeroex_v2_ethereum', 'Exchange2_1_evt_Fill') }} fills
        LEFT JOIN {{ source('prices', 'usd') }} tp ON
            date_trunc('minute', evt_block_time) = tp.minute and tp.blockchain = 'ethereum'
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills.takerAssetData,17,20) IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    ELSE SUBSTRING(fills.takerAssetData,17,20)
                END = tp.contract_address
        LEFT JOIN {{ source('prices', 'usd') }} mp ON
            DATE_TRUNC('minute', evt_block_time) = mp.minute and mp.blockchain = 'ethereum'
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN SUBSTRING(fills.makerAssetData,17,20) IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    ELSE SUBSTRING(fills.makerAssetData,17,20)
                END = mp.contract_address
        LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} mt ON mt.contract_address = SUBSTRING(fills.makerAssetData,17,20)
        LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} tt ON tt.contract_address = SUBSTRING(fills.takerAssetData,17,20)
         where 1=1  
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}
    )
    , */ v4_limit_fills AS (

        SELECT
            fills.evt_block_time AS block_time, fills.evt_block_number as block_number
            , 'v4' AS protocol_version
            , fills.evt_tx_hash AS transaction_hash
            , fills.evt_index
            , fills.maker AS maker_address
            , fills.taker AS taker_address
            , fills.makerToken AS maker_token
            , mt.symbol AS maker_symbol
            , CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair
            , fills.takerTokenFilledAmount as taker_token_filled_amount_raw
            , fills.makerTokenFilledAmount as maker_token_filled_amount_raw
            , fills.makerTokenFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount
            , fills.takerToken AS taker_token
            , tt.symbol AS taker_symbol
            , fills.takerTokenFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount
            , (fills.feeRecipient in 
                ('0x9b858be6e3047d88820f439b240deac2418a2551','0x86003b044f70dac0abc80ac8957305b6370893ed','0x5bc2419a087666148bfbe1361ae6c06d240c6131')) 
                AS matcha_limit_order_flag
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
                  ELSE COALESCE((fills.makerTokenFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerTokenFilledAmount / pow(10, tt.decimals))*tp.price)
                END AS volume_usd
            , fills.protocolFeePaid/ 1e18 AS protocol_fee_paid_eth
            , fills.contract_address
            , 'limit' as native_order_type
        FROM {{ source('zeroex_ethereum', 'ExchangeProxy_evt_LimitOrderFilled') }} fills
        LEFT JOIN {{ source('prices', 'usd') }} tp ON 
            date_trunc('minute', evt_block_time) = tp.minute and tp.blockchain = 'ethereum'
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN fills.takerToken IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    ELSE fills.takerToken
                END = tp.contract_address
        LEFT JOIN {{ source('prices', 'usd') }} mp ON 
            DATE_TRUNC('minute', evt_block_time) = mp.minute and    mp.blockchain = 'ethereum'
            AND CASE
                    -- Set Deversifi ETHWrapper to WETH
                    WHEN fills.makerToken IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    ELSE fills.makerToken
                END = mp.contract_address
        LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} mt ON mt.contract_address = fills.makerToken
        LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} tt ON tt.contract_address = fills.takerToken
         where 1=1  
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}
    )

    , v4_rfq_fills AS (
      SELECT
          fills.evt_block_time AS block_time, fills.evt_block_number as block_number
          , 'v4' AS protocol_version
          , fills.evt_tx_hash AS transaction_hash
          , fills.evt_index
          , fills.maker AS maker_address
          , fills.taker AS taker_address
          , fills.makerToken AS maker_token
          , mt.symbol AS maker_symbol
          , CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair
          , fills.takerTokenFilledAmount as taker_token_filled_amount_raw
          , fills.makerTokenFilledAmount as maker_token_filled_amount_raw
          , fills.makerTokenFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount
          , fills.takerToken AS taker_token
          , tt.symbol AS taker_symbol
          , fills.takerTokenFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount
          , false as matcha_limit_order_flag
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
                  ELSE COALESCE((fills.makerTokenFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerTokenFilledAmount / pow(10, tt.decimals))*tp.price)
              END AS volume_usd
          , cast(null as numeric) AS protocol_fee_paid_eth,
          fills.contract_address
          , 'rfq' as native_order_type
      FROM {{ source('zeroex_ethereum', 'ExchangeProxy_evt_RfqOrderFilled') }} fills
      LEFT JOIN {{ source('prices', 'usd') }} tp ON
          date_trunc('minute', evt_block_time) = tp.minute 
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                    WHEN fills.takerToken IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    ELSE fills.takerToken
              END = tp.contract_address
      LEFT JOIN {{ source('prices', 'usd') }} mp ON
          DATE_TRUNC('minute', evt_block_time) = mp.minute 
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                    WHEN fills.makerToken IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    ELSE fills.makerToken
              END = mp.contract_address
      LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} mt ON mt.contract_address = fills.makerToken
      LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} tt ON tt.contract_address = fills.takerToken
       where 1=1  
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}
    ), otc_fills as
    (
      SELECT
          fills.evt_block_time AS block_time, fills.evt_block_number as block_number
          , 'v4' AS protocol_version
          , fills.evt_tx_hash AS transaction_hash
          , fills.evt_index
          , fills.maker AS maker_address
          , fills.taker AS taker_address
          , fills.makerToken AS maker_token
          , mt.symbol AS maker_symbol
          , CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair
          , fills.takerTokenFilledAmount as taker_token_filled_amount_raw
          , fills.makerTokenFilledAmount as maker_token_filled_amount_raw
          , fills.makerTokenFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount
          , fills.takerToken AS taker_token
          , tt.symbol AS taker_symbol
          , fills.takerTokenFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount
          , false as matcha_limit_order_flag
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
                  ELSE COALESCE((fills.makerTokenFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerTokenFilledAmount / pow(10, tt.decimals))*tp.price)
              END AS volume_usd
          , cast(null as numeric) AS protocol_fee_paid_eth
          , fills.contract_address
          , 'otc' as native_order_type
      FROM {{ source('zeroex_ethereum', 'ExchangeProxy_evt_OtcOrderFilled') }} fills
      LEFT JOIN {{ source('prices', 'usd') }} tp ON
          date_trunc('minute', evt_block_time) = tp.minute and tp.blockchain = 'ethereum'
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                    WHEN fills.takerToken IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    ELSE fills.takerToken
              END = tp.contract_address
      LEFT JOIN {{ source('prices', 'usd') }} mp ON
          DATE_TRUNC('minute', evt_block_time) = mp.minute  and mp.blockchain = 'ethereum'
          AND CASE
                  -- Set Deversifi ETHWrapper to WETH
                    WHEN fills.makerToken IN ('0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                    ELSE fills.makerToken
              END = mp.contract_address
      LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} mt ON mt.contract_address = fills.makerToken
      LEFT OUTER JOIN {{ ref('tokens_ethereum_erc20_legacy') }} tt ON tt.contract_address = fills.takerToken
       where 1=1  
                {% if is_incremental() %}
                AND evt_block_time >= date_trunc('day', now() - interval '1 week')
                {% endif %}
                {% if not is_incremental() %}
                AND evt_block_time >= '{{zeroex_v3_start_date}}'
                {% endif %}

    ),

    all_fills as (
    
    /*SELECT * FROM v3_fills

    UNION ALL

    SELECT * FROM v2_1_fills

    UNION ALL */

    SELECT * FROM v4_limit_fills

    UNION ALL

    SELECT * FROM v4_rfq_fills

    UNION ALL
    
    SELECT * FROM otc_fills
    )
            SELECT distinct 
                all_fills.block_time AS block_time, 
                all_fills.block_number,
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
                token_pair,
                '1' as trace_address,
                maker_asset_filled_amount maker_token_amount,
                taker_token, 
                taker_symbol,
                taker_asset_filled_amount taker_token_amount,
                matcha_limit_order_flag,
                volume_usd,
                cast(protocol_fee_paid_eth as double),
                'ethereum' as blockchain,
                all_fills.contract_address,
                native_order_type,
                tx.from AS tx_from,
                tx.to AS tx_to
            FROM all_fills
            INNER JOIN {{ source('ethereum', 'transactions')}} tx ON all_fills.transaction_hash = tx.hash
            AND all_fills.block_number = tx.block_number
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
            {% if not is_incremental() %}
            AND tx.block_time >= '{{zeroex_v3_start_date}}'
            {% endif %}