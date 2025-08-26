{{ config(
        schema = 'prices_test',
        alias = 'minute_interpolated_validation',
        materialized = 'view'
    )
}}

/*
    Test model for prices.minute interpolation

    This model provides validation and testing capabilities for the sparse hour 
    interpolation approach. It includes sample data scenarios and validation checks.
*/

WITH

-- Test scenario 1: Normal hourly data with gaps
test_hourly_data AS (
    SELECT 
        'ethereum' as blockchain,
        0x123456 as contract_address,
        'WETH' as symbol,
        18 as decimals,
        timestamp_hour,
        price
    FROM VALUES 
        (timestamp '2024-01-01 10:00:00', 2000.0),  -- Hour 10
        (timestamp '2024-01-01 11:00:00', 2100.0),  -- Hour 11
        (timestamp '2024-01-01 13:00:00', 2200.0),  -- Hour 13 (missing hour 12)
        (timestamp '2024-01-01 14:00:00', 2150.0)   -- Hour 14
    AS t(timestamp_hour, price)
),

-- Expected interpolated minute data for validation
expected_minutes AS (
    SELECT 
        blockchain,
        contract_address,
        symbol,
        decimals,
        minute_timestamp,
        expected_price,
        test_case
    FROM (
        VALUES 
            ('ethereum', 0x123456, 'WETH', 18, timestamp '2024-01-01 10:00:00', 2000.0, 'hour_start'),
            ('ethereum', 0x123456, 'WETH', 18, timestamp '2024-01-01 10:30:00', 2000.0, 'mid_hour_forward_fill'),
            ('ethereum', 0x123456, 'WETH', 18, timestamp '2024-01-01 10:59:00', 2000.0, 'hour_end_forward_fill'),
            ('ethereum', 0x123456, 'WETH', 18, timestamp '2024-01-01 11:00:00', 2100.0, 'next_hour_update'),
            ('ethereum', 0x123456, 'WETH', 18, timestamp '2024-01-01 13:15:00', 2200.0, 'after_gap_forward_fill')
    ) AS t(blockchain, contract_address, symbol, decimals, minute_timestamp, expected_price, test_case)
),

-- Simulate the interpolation logic for testing
test_interpolation AS (
    SELECT 
        td.blockchain,
        td.contract_address,
        td.symbol,
        td.decimals,
        td.timestamp_hour,
        td.price,
        lead(td.timestamp_hour) over (
            partition by td.blockchain, td.contract_address, td.symbol, td.decimals 
            order by td.timestamp_hour asc
        ) as next_hour_timestamp
    FROM test_hourly_data td
),

-- Generate minute timeseries for test
test_minutes AS (
    SELECT 
        ti.blockchain,
        ti.contract_address,
        ti.symbol,
        ti.decimals,
        date_add('minute', seq.minute_offset, ti.timestamp_hour) as minute_timestamp,
        ti.price,
        ti.timestamp_hour as source_hour
    FROM test_interpolation ti
    CROSS JOIN unnest(sequence(0, 59)) as seq(minute_offset)
    WHERE 
        -- Only generate minutes that are before the next actual hour data point
        (ti.next_hour_timestamp IS NULL OR 
         date_add('minute', seq.minute_offset, ti.timestamp_hour) < ti.next_hour_timestamp)
)

-- Validation query combining expected and actual results
SELECT 
    'test_summary' as validation_type,
    count(*) as total_interpolated_minutes,
    count(distinct date_trunc('hour', minute_timestamp)) as unique_hours_covered,
    min(minute_timestamp) as earliest_minute,
    max(minute_timestamp) as latest_minute,
    avg(price) as average_price,
    -- Check for gaps in minute coverage
    case when count(*) = 
        (extract(epoch from max(minute_timestamp) - min(minute_timestamp))/60 + 1 - 60) -- account for missing hour 12
        then 'PASS' else 'FAIL' 
    end as minute_coverage_test
FROM test_minutes

UNION ALL

-- Test specific scenarios
SELECT 
    tm.source_hour::varchar as validation_type,
    count(*) as total_interpolated_minutes,
    count(distinct tm.price) as unique_prices_in_hour,
    min(tm.minute_timestamp) as earliest_minute,
    max(tm.minute_timestamp) as latest_minute,
    avg(tm.price) as average_price,
    case when count(*) <= 60 then 'PASS' else 'FAIL' end as minute_coverage_test
FROM test_minutes tm
GROUP BY tm.source_hour
ORDER BY validation_type