{{  config(
        
        schema = 'zeroex_celo',
        alias = 'api_fills_deduped',
        materialized='incremental',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        on_schema_change='sync_all_columns',
        file_format ='delta',
        incremental_strategy='merge'
    )
}}

{% set zeroex_v3_start_date = '2019-12-01' %}

WITH fills_with_tx_fill_number
AS
(
    SELECT   row_number() OVER ( partition BY tx_hash ORDER BY evt_index ASC ) AS tx_fill_number
           , *
    FROM {{ ref('zeroex_celo_api_fills') }}
    WHERE 1=1
    AND swap_flag = true 
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND block_time >= cast('{{zeroex_v3_start_date}}' as date) 
    {% endif %}    
)
, fills_first_last
AS
(
    SELECT CASE
               WHEN a.taker_token = c.maker_token AND a.taker_token_amount_raw = c.maker_token_amount_raw THEN 0
               ELSE 1 END AS                                                           taker_consider_flag--from
         , CASE
               WHEN a.maker_token = b.taker_token AND a.maker_token_amount_raw = b.taker_token_amount_raw THEN 0
               ELSE 1 END AS                                                           maker_consider_flag
         , SUM(CASE
                   WHEN a.maker_token = b.taker_token AND a.maker_token_amount_raw = b.taker_token_amount_raw THEN 0
                   ELSE 1 END) OVER (PARTITION BY a.tx_hash ORDER BY a.evt_index DESC) hop_count
         , a.*
    FROM  fills_with_tx_fill_number a
        LEFT JOIN fills_with_tx_fill_number b ON (a.tx_hash = b.tx_hash AND a.tx_fill_number = b.tx_fill_number - 1)
        LEFT JOIN fills_with_tx_fill_number c ON (a.tx_hash = c.tx_hash AND a.tx_fill_number = c.tx_fill_number + 1)
)
, deduped_bridge_fills
AS
(
    SELECT   tx_hash
        , MAX(evt_index)                                                                    AS evt_index
        , MAX(affiliate_address)                                                            AS affiliate_address
        , MAX(CASE WHEN taker_consider_flag = 0 THEN NULL ELSE taker_token END )            AS taker_token
        , MAX(CASE WHEN maker_consider_flag = 0 THEN NULL ELSE maker_token END )            AS maker_token
        , MAX(CASE WHEN taker_consider_flag = 0 THEN NULL ELSE taker_symbol END )           AS taker_symbol
        , MAX(CASE WHEN maker_consider_flag = 0 THEN NULL ELSE maker_symbol END )           AS maker_symbol        
        , MAX(CASE WHEN taker_consider_flag = 0 THEN NULL ELSE taker_token_amount END)      AS taker_token_amount
        , MAX(CASE WHEN maker_consider_flag = 0 THEN NULL ELSE maker_token_amount END)      AS maker_token_amount
        , MAX(CASE WHEN taker_consider_flag = 0 THEN NULL ELSE taker_token_amount_raw END)  AS taker_token_amount_raw
        , MAX(CASE WHEN maker_consider_flag = 0 THEN NULL ELSE maker_token_amount_raw END)  AS maker_token_amount_raw
        , COUNT(*)                                                                          AS fills_within
    FROM fills_first_last a
    GROUP BY  tx_hash,hop_count
)
SELECT  a.blockchain
      , '0x API'  as project
      , cast(null as varchar(10)) as version
      , a.block_date
      , CAST(date_trunc('month', a.block_time) as DATE) as block_month 
      , a.block_time
      , b.taker_symbol AS taker_symbol
      , b.maker_symbol AS maker_symbol
      , CASE
        WHEN LOWER(b.taker_symbol) > LOWER(b.maker_symbol)
        THEN CONCAT(COALESCE(b.maker_symbol, ''), '-', COALESCE(b.taker_symbol, ''))
        ELSE CONCAT(COALESCE(b.taker_symbol, ''), '-', COALESCE(b.maker_symbol, ''))
        END AS token_pair   
      , b.taker_token_amount
      , b.maker_token_amount
      , b.taker_token_amount_raw AS taker_token_amount_raw
      , b.maker_token_amount_raw AS maker_token_amount_raw
      , a.volume_usd
      , b.taker_token
      , b.maker_token
      , a.taker
      , a.maker
      , a.affiliate_address
      , a.tx_hash
      , a.tx_from
      , a.tx_to
      , b.evt_index
      , ARRAY[-1] AS trace_address
      , a.type
      , a.swap_flag
      , b.fills_within
      , a.contract_address 
FROM fills_with_tx_fill_number a
INNER JOIN deduped_bridge_fills b
    ON a.tx_hash = b.tx_hash AND a.evt_index = b.evt_index