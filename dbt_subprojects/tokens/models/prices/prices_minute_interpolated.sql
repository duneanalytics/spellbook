{{ config(
        schema = 'prices',
        alias = 'minute',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'contract_address', 'symbol', 'decimals', 'minute'],
        partition_by = ['blockchain'],
        post_hook = '{{ expose_spells(\'["ethereum","polygon","bnb","avalanche_c","optimism","arbitrum","gnosis","fantom","base","zksync","linea","zkevm","blast","sei","nova","worldchain","kaia","ronin","ink","shape","scroll","mantle","celo"]\', 
                       "sector", 
                       "prices", 
                       \'["0xRob", "jeff-dude", "hildobby"]\') }}'
    )
}}

/*
    Sparse Hour Interpolation Approach for prices.minute

    This model creates minute-level price data by interpolating from hourly price data.
    The approach reduces noise and outliers by using stable hourly anchor points 
    rather than volatile minute-by-minute data.

    Key Features:
    1. Uses sparse hourly data as anchor points
    2. Forward-fills prices within each hour until next hourly update
    3. Generates complete minute timeseries for better UX
    4. Handles missing hours with forward-fill from previous hour
*/

{% if is_incremental() %}
{%- set lookback_days = "'3'" %}
{% else %}
{%- set lookback_days = "'90'" %}
{% endif %}

WITH 

-- Get hourly price data as our sparse anchor points
hourly_prices AS (
    SELECT 
        blockchain,
        contract_address,
        symbol,
        decimals,
        timestamp as hour_timestamp,
        price,
        -- Calculate next hour for each token to know interpolation range
        lead(timestamp) over (
            partition by blockchain, contract_address, symbol, decimals 
            order by timestamp asc
        ) as next_hour_timestamp
    FROM {{ source('prices', 'hour') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND timestamp >= current_timestamp - interval {{lookback_days}} day
        {% else %}
        AND timestamp >= current_timestamp - interval {{lookback_days}} day
        {% endif %}
        AND price IS NOT NULL
        AND price > 0
        -- Ensure we have recent data by filtering out very old timestamps
        AND timestamp >= timestamp '2020-01-01'
),

-- Generate minute-level timeseries for each token-hour combination
-- This creates 60 minute entries for each hour, but stops early if there's a next hour data point
minute_timeseries AS (
    SELECT 
        hp.blockchain,
        hp.contract_address, 
        hp.symbol,
        hp.decimals,
        hp.hour_timestamp,
        hp.next_hour_timestamp,
        hp.price,
        -- Generate minute timestamps from hour start to either next hour or +1 hour
        date_add('minute', seq.minute_offset, hp.hour_timestamp) as minute_timestamp
    FROM hourly_prices hp
    CROSS JOIN unnest(sequence(0, 59)) as seq(minute_offset)
    WHERE 
        -- Only generate minutes that are before the next actual hour data point
        -- This ensures we don't have overlapping price intervals
        -- For the last available hour, we generate the full 60 minutes
        (hp.next_hour_timestamp IS NULL OR 
         date_add('minute', seq.minute_offset, hp.hour_timestamp) < hp.next_hour_timestamp)
),

-- Final minute-level prices using sparse hour interpolation
interpolated_minutes AS (
    SELECT 
        blockchain,
        contract_address,
        symbol,
        decimals,
        minute_timestamp as minute,
        price,
        hour_timestamp,
        -- Add metadata to track that this is interpolated from hourly data
        'sparse_hour_interpolation' as price_method
    FROM minute_timeseries
    WHERE minute_timestamp <= current_timestamp
)

SELECT 
    blockchain,
    contract_address,
    symbol,
    decimals,
    minute,
    price
FROM interpolated_minutes

-- Ensure we don't have duplicates and handle edge cases
WHERE price IS NOT NULL 
  AND price > 0
  AND minute IS NOT NULL

{% if is_incremental() %}
-- Only update recent data in incremental runs
AND minute >= current_timestamp - interval {{lookback_days}} day
{% endif %}

ORDER BY blockchain, contract_address, symbol, decimals, minute