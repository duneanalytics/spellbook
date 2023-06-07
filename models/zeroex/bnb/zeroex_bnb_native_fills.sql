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
            fills.evt_block_time AS block_time, fills.evt_block_number as block_number
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
            , CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair
            , fills.makerTokenFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount
            , fills.takerToken AS taker_token
            , tt.symbol AS taker_symbol
            , fills.takerTokenFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount
            , (fills.feeRecipient in 
                ('0x9b858be6e3047d88820f439b240deac2418a2551','0x86003b044f70dac0abc80ac8957305b6370893ed','0x5bc2419a087666148bfbe1361ae6c06d240c6131')) 
                AS matcha_limit_order_flag
            , COALESCE((fills.makerTokenFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerTokenFilledAmount / pow(10, tt.decimals))*tp.price) AS volume_usd
            , fills.protocolFeePaid/ 1e18 AS protocol_fee_paid_eth
        FROM {{ source('zeroex_bnb', 'ExchangeProxy_evt_LimitOrderFilled') }} fills
        LEFT JOIN {{ source('prices', 'usd') }} tp ON
            date_trunc('minute', evt_block_time) = tp.minute and  tp.blockchain = 'bnb'
            AND CASE
                    -- set native token to wrapped version
                    WHEN fills.takerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
                    ELSE fills.takerToken
                END = tp.contract_address
        LEFT JOIN {{ source('prices', 'usd') }} mp ON 
            DATE_TRUNC('minute', evt_block_time) = mp.minute  and  mp.blockchain = 'bnb'
            AND CASE
                    -- set native token to wrapped version
                    WHEN fills.makerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
                    ELSE fills.makerToken
                END = mp.contract_address
        LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = fills.makerToken and mt.blockchain = 'bnb'
        LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = fills.takerToken and tt.blockchain = 'bnb'
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
          , CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair
          , fills.makerTokenFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount
          , fills.takerToken AS taker_token
          , tt.symbol AS taker_symbol
          , fills.takerTokenFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount
          , FALSE AS matcha_limit_order_flag
          , COALESCE((fills.makerTokenFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerTokenFilledAmount / pow(10, tt.decimals))*tp.price) AS volume_usd
          , cast(NULL as numeric) AS protocol_fee_paid_eth
      FROM {{ source('zeroex_bnb', 'ExchangeProxy_evt_RfqOrderFilled') }} fills
      LEFT JOIN {{ source('prices', 'usd') }} tp ON
          date_trunc('minute', evt_block_time) = tp.minute and  tp.blockchain = 'bnb'
          AND CASE
                  -- set native token to wrapped version
                    WHEN fills.takerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
                    ELSE fills.takerToken
              END = tp.contract_address
      LEFT JOIN {{ source('prices', 'usd') }} mp ON 
          DATE_TRUNC('minute', evt_block_time) = mp.minute  and  mp.blockchain = 'bnb'
          AND CASE
                  -- set native token to wrapped version
                    WHEN fills.makerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
                    ELSE fills.makerToken
              END = mp.contract_address
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = fills.makerToken and mt.blockchain = 'bnb'
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = fills.takerToken and tt.blockchain = 'bnb'
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
          , CASE WHEN lower(tt.symbol) > lower(mt.symbol) THEN concat(mt.symbol, '-', tt.symbol) ELSE concat(tt.symbol, '-', mt.symbol) END AS token_pair
          , fills.makerTokenFilledAmount / pow(10, mt.decimals) AS maker_asset_filled_amount
          , fills.takerToken AS taker_token
          , tt.symbol AS taker_symbol
          , fills.takerTokenFilledAmount / pow(10, tt.decimals) AS taker_asset_filled_amount
          , FALSE AS matcha_limit_order_flag
          , COALESCE((fills.makerTokenFilledAmount / pow(10, mt.decimals))*mp.price,(fills.takerTokenFilledAmount / pow(10, tt.decimals))*tp.price) AS volume_usd
          , cast(null as numeric) AS protocol_fee_paid_eth
        FROM {{ source('zeroex_bnb', 'ExchangeProxy_evt_OtcOrderFilled') }} fills
      LEFT JOIN {{ source('prices', 'usd') }} tp ON
          date_trunc('minute', evt_block_time) = tp.minute and tp.blockchain = 'bnb'
          AND CASE
                  -- set native token to wrapped version
                    WHEN fills.takerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
                    ELSE fills.takerToken
              END = tp.contract_address
      LEFT JOIN {{ source('prices', 'usd') }} mp ON 
          DATE_TRUNC('minute', evt_block_time) = mp.minute  and mp.blockchain = 'bnb'
          AND CASE
                  -- set native token to wrapped version
                    WHEN fills.makerToken = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
                    ELSE fills.makerToken
              END = mp.contract_address
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} mt ON mt.contract_address = fills.makerToken and mt.blockchain = 'bnb'
      LEFT OUTER JOIN {{ ref('tokens_erc20') }} tt ON tt.contract_address = fills.takerToken and tt.blockchain = 'bnb'
       where 1=1  
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
                all_fills.block_time AS block_time, 
                all_fills.block_number as block_number,
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
                'bnb' as blockchain,
                all_fills.contract_address,
                native_order_type,
                tx.from AS tx_from,
                tx.to AS tx_to
            FROM all_fills
            INNER JOIN {{ source('bnb', 'transactions')}} tx ON all_fills.transaction_hash = tx.hash
            AND all_fills.block_number = tx.block_number
            {% if is_incremental() %}
            AND tx.block_time >= date_trunc('day', now() - interval '1 week')
            {% endif %}
            {% if not is_incremental() %}
            AND tx.block_time >= '{{zeroex_v3_start_date}}'
            {% endif %}
            