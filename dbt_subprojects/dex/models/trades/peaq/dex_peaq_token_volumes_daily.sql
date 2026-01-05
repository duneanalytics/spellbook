{{ config(
    schema = 'dex_peaq'
    , alias = 'token_volumes_daily'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'token_address', 'symbol', 'block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}

WITH dex_trades_filtered AS (
  SELECT *
  FROM {{ ref('dex_trades') }} t
  WHERE blockchain = 'peaq'
  {% if var('dev_dates', false) -%}
  AND block_date > current_date - interval '3' day -- dev_dates mode for dev, to prevent full scan
  {%- elif is_incremental() -%}
  AND {{ incremental_predicate('block_date') }}
  {%- endif %}
),

daily_flows AS (
    --volumes bought
    SELECT  
        blockchain
      , block_month
      , block_date
      , token_bought_address AS token_address
      , coalesce(token_bought_symbol, '') AS symbol
      , SUM(token_bought_amount_raw) AS bought_volume_raw
      , SUM(token_bought_amount) AS bought_volume
      , SUM(amount_usd) AS bought_volume_usd
      , CAST(NULL AS double) AS sold_volume_raw
      , CAST(NULL AS double) AS sold_volume
      , CAST(NULL AS double) AS sold_volume_usd
    FROM dex_trades_filtered
    GROUP BY blockchain, block_month, block_date, token_bought_address, coalesce(token_bought_symbol, '')

    UNION ALL

    --volumes sold
    SELECT  
        blockchain
      , block_month
      , block_date
      , token_sold_address AS token_address
      , coalesce(token_sold_symbol, '') AS symbol
      , CAST(NULL AS double) AS bought_volume_raw
      , CAST(NULL AS double) AS bought_volume
      , CAST(NULL AS double) AS bought_volume_usd
      , SUM(token_sold_amount_raw) AS sold_volume_raw
      , SUM(token_sold_amount) AS sold_volume
      , SUM(amount_usd) AS sold_volume_usd
    FROM dex_trades_filtered
    GROUP BY blockchain, block_month, block_date, token_sold_address, coalesce(token_sold_symbol, '')
),

sums AS (
  SELECT
    blockchain
  , block_month
  , block_date
  , token_address
  , symbol
  , SUM(bought_volume_raw) AS bought_volume_raw_sum
  , SUM(sold_volume_raw) AS sold_volume_raw_sum
  , SUM(bought_volume) AS bought_volume_sum
  , SUM(sold_volume) AS sold_volume_sum
  , SUM(bought_volume_usd) AS bought_volume_usd_sum
  , SUM(sold_volume_usd) AS sold_volume_usd_sum
  FROM daily_flows
  GROUP BY blockchain, block_month, block_date, token_address, symbol
)

SELECT
    blockchain
  , block_month
  , block_date
  , token_address
  , symbol
  , CASE WHEN bought_volume_raw_sum IS NULL AND sold_volume_raw_sum IS NULL THEN NULL 
         ELSE COALESCE(bought_volume_raw_sum, 0) + COALESCE(sold_volume_raw_sum, 0) 
    END AS volume_raw
  , CASE WHEN bought_volume_sum IS NULL AND sold_volume_sum IS NULL THEN NULL 
         ELSE COALESCE(bought_volume_sum, 0) + COALESCE(sold_volume_sum, 0) 
    END AS volume
  , CASE WHEN bought_volume_usd_sum IS NULL AND sold_volume_usd_sum IS NULL THEN NULL 
         ELSE COALESCE(bought_volume_usd_sum, 0) + COALESCE(sold_volume_usd_sum, 0) 
    END AS volume_usd
FROM sums

