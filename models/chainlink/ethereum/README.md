## OCR Metrics Example

Aggregate OCR data monthly with margin and  aggregation, along with margin by node operator

```sql
WITH 
  reward as (
    SELECT
      date_trunc('month', date_start) as date_start,
      operator_name,
      SUM(token_amount) as reward_token,
      SUM(usd_amount) as reward_usd
    FROM test_schema.git_dunesql_1294d1da_chainlink_ethereum_ocr_reward_daily
    WHERE operator_name = 'LinkPool'
      AND date_start >= cast('2023-01-01' as date)
    GROUP BY 1, 2
  ),
  gas as (
    SELECT
      date_trunc('month', date_start) as date_start,
      operator_name,
      SUM(total_token_amount) as gas_token,
      SUM(total_usd_amount) as gas_usd
    FROM test_schema.git_dunesql_1294d1da_chainlink_ethereum_ocr_gas_daily
    GROUP BY 1, 2
  ),
  request as (
    SELECT
      date_trunc('month', date_start) as date_start,
      operator_name,
      SUM(fulfilled_requests) as fulfilled_requests,
      SUM(reverted_requests) as reverted_requests,
      SUM(total_requests) as total_requests
    FROM test_schema.git_dunesql_1294d1da_chainlink_ethereum_ocr_request_daily
    GROUP BY 1, 2
  )
SELECT 
  reward.date_start,
  reward.operator_name,
  reward.reward_token,
  reward.reward_usd,
  gas.gas_token,
  gas.gas_usd,
  (reward.reward_usd - gas.gas_usd) as margin_usd,
  (reward.reward_usd - gas.gas_usd) / reward.reward_usd as margin_rate,
  request.fulfilled_requests,
  request.reverted_requests,
  request.total_requests,
  CAST(request.reverted_requests AS REAL) / request.total_requests as revert_rate
FROM 
  reward
LEFT JOIN gas ON
  gas.date_start = reward.date_start AND
  gas.operator_name = reward.operator_name
LEFT JOIN request ON
  request.date_start = reward.date_start AND
  request.operator_name = reward.operator_name
ORDER BY 1, 2
```