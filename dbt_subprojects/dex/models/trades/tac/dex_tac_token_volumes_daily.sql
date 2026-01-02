{{ config(
    schema = 'dex_tac'
    , alias = 'token_volumes_daily'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'token_address', 'block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}

WITH trusted_tokens AS (
  SELECT contract_address
  FROM {{ source('prices','trusted_tokens') }}
  WHERE blockchain = 'tac'
),

dex_trades_filtered AS (
  SELECT *
  FROM {{ ref('dex_tac_trades') }} t
  {% if var('dev_dates', false) -%}
  WHERE block_date > current_date - interval '3' day -- dev_dates mode for dev, to prevent full scan
  {%- elif is_incremental() -%}
  WHERE {{ incremental_predicate('block_date') }}
  {%- endif %}
),

daily_flows AS (
    --volumes bought
    SELECT  
        blockchain
      , block_month
      , block_date
      , token_bought_address AS token_address
      , token_bought_symbol AS symbol
      , SUM(token_bought_amount_raw) AS bought_volume_raw
      , SUM(token_bought_amount) AS bought_volume
      , SUM(CASE WHEN token_bought_address IN (SELECT * FROM trusted_tokens) THEN amount_usd END) AS bought_volume_usd --only getting usd volume for trusted tokens
      , CAST(NULL AS double) AS sold_volume_raw
      , CAST(NULL AS double) AS sold_volume
      , CAST(NULL AS double) AS sold_volume_usd
    FROM dex_trades_filtered
    GROUP BY blockchain, block_month, block_date, token_bought_address, token_bought_symbol

    UNION ALL

    --volumes sold
    SELECT  
        blockchain
      , block_month
      , block_date
      , token_sold_address AS token_address
      , token_sold_symbol AS symbol
      , CAST(NULL AS double) AS bought_volume_raw
      , CAST(NULL AS double) AS bought_volume
      , CAST(NULL AS double) AS bought_volume_usd
      , SUM(token_sold_amount_raw) AS sold_volume_raw
      , SUM(token_sold_amount) AS sold_volume
      , SUM(CASE WHEN token_sold_address IN (SELECT * FROM trusted_tokens) THEN amount_usd END) AS sold_volume_usd --only getting usd volume for trusted tokens
    FROM dex_trades_filtered
    GROUP BY blockchain, block_month, block_date, token_sold_address, token_sold_symbol
)

SELECT
    blockchain
  , block_month
  , block_date
  , token_address
  , symbol
  , SUM(bought_volume_raw) + SUM(sold_volume_raw) AS volume_raw
  , SUM(bought_volume) + SUM(sold_volume) AS volume
  , SUM(bought_volume_usd) + SUM(sold_volume_usd) AS volume_usd
FROM daily_flows
GROUP BY blockchain, block_month, block_date, token_address, symbol

