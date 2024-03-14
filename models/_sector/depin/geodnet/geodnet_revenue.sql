{{ config(
    schema = 'depin'
    , alias = 'geodnet_revenue'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['date', 'chain', 'name']
    )
}}

WITH
  hour_series AS (
    SELECT
      time AS hour,
      DATE_TRUNC('day', time) AS day
    FROM
      UNNEST (
        sequence(
          CAST('2023-04-20 00:00' AS timestamp),
          CAST(
            DATE_TRUNC('hour', CURRENT_TIMESTAMP) AS timestamp
          ),
          INTERVAL '1' hour
        )
      ) AS t (time)
      {% if is_incremental() %}
      WHERE
        time > (
          SELECT
            COALESCE(MAX(date), '2023-04-20 00:00')
          FROM
            {{ this }}
        )
      {% endif %}
  ),
  burn_events AS (
    SELECT
      DATE_TRUNC('hour', a.evt_block_time) AS evt_hour,
      DATE_TRUNC('day', a.evt_block_time) AS evt_day,
      CAST(value AS double) / pow(10, b.decimals) AS tokens_burned
    FROM {{ source('erc20_polygon', 'evt_transfer') }} a
      JOIN {{ source('tokens', 'erc20') }} b ON a.contract_address = b.contract_address
    WHERE
      a.contract_address = 0xAC0F66379A6d7801D7726d5a943356A172549Adb
      AND a.to = 0x000000000000000000000000000000000000dead
      {% if is_incremental() %}
      AND a.evt_block_time > (
        SELECT
          COALESCE(MAX(date), '2023-04-20 00:00')
        FROM
          {{ this }}
      )
      {% endif %}
  ),
  raw_prices AS (
    SELECT
      AVG(median_price) AS price,
      DATE_TRUNC('hour', hour) AS hour,
      DATE_TRUNC('day', hour) AS day
    FROM {{ ref('dex_prices') }}
    WHERE
      blockchain = 'polygon'
      AND contract_address = 0xac0f66379a6d7801d7726d5a943356a172549adb
      {% if is_incremental() %}
      AND hour > (
        SELECT
          COALESCE(MAX(date), '2023-04-20 00:00')
        FROM
          {{ this }}
      )
      {% endif %}
    GROUP BY
      2,
      3
  ),
  price_data_with_imputation AS (
    SELECT
      hs.hour,
      hs.day,
      COALESCE(
        rp.price,
        LAG(rp.price) OVER (
          ORDER BY
            hs.hour
        ),
        LEAD(rp.price) OVER (
          ORDER BY
            hs.hour
        )
      ) AS imputed_price
    FROM
      hour_series hs
      LEFT JOIN raw_prices rp ON hs.hour = rp.hour
  ),
  hourly_burned_value AS (
    SELECT
      hs.hour,
      hs.day,
      COALESCE(SUM(be.tokens_burned), 0) AS hourly_tokens_burned,
      COALESCE(SUM(be.tokens_burned * pdi.imputed_price), 0) AS hourly_value_burned
    FROM
      hour_series hs
      LEFT JOIN burn_events be ON hs.hour = be.evt_hour
      LEFT JOIN price_data_with_imputation pdi ON hs.hour = pdi.hour
    GROUP BY
      hs.hour,
      hs.day
  ),
  daily_burned_value AS (
    SELECT
      day,
      SUM(hourly_tokens_burned) AS tokens_burned,
      SUM(hourly_value_burned) AS usd_burned
    FROM
      hourly_burned_value
    GROUP BY
      day
  )
SELECT
  DATE_FORMAT(mbv.day, '%Y-%m-%d') AS date,
  'polygon' as chain,
  'geodnet' as name,
   mbv.usd_burned as revenue
FROM
  daily_burned_value mbv
ORDER BY
  1 DESC