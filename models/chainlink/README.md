# Price Feeds

## Optimism 

To add a new price feed, add a row to the `chainlink_optimism_price_feeds_oracle_addresses.sql` table with the following information:
- **feed_name**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/)
- **decimals**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/) (Note: This refers to the price feed decimals, not the underlying token's decimals)
- **proxy_address**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/)
- **aggregator_address**: Sourced from opening the proxy address on Etherscan ([example](https://optimistic.etherscan.io/address/0x338ed6787f463394D24813b297401B9F05a8C9d1#readContract)), clicking 'Contract' -> 'Read Contract' and getting the address from the 'aggregator' field

*Open Research Area: Is there a way for us to deterministically build the oracle -> address -> feed name -> token links purely by reading on-chain events vs manually entering data from Chainlink docs?*
# Price Feeds

## Optimism 

To add a new price feed, add a row to the `chainlink_optimism_price_feeds_oracle_addresses.sql` table with the following information:
- **feed_name**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/)
- **decimals**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/) (Note: This refers to the price feed decimals, not the underlying token's decimals)
- **proxy_address**: Sourced from [Chainlink Docs](https://docs.chain.link/docs/optimism-price-feeds/)
- **aggregator_address**: Sourced from opening the proxy address on Etherscan ([example](https://optimistic.etherscan.io/address/0x338ed6787f463394D24813b297401B9F05a8C9d1#readContract)), clicking 'Contract' -> 'Read Contract' and getting the address from the 'aggregator' field

*Open Research Area: Is there a way for us to deterministically build the oracle -> address -> feed name -> token links purely by reading on-chain events vs manually entering data from Chainlink docs?*

## OCR Metrics Usage Example

Aggregate OCR data monthly with margin and  aggregation, along with margin by node operator

```sql
WITH 
  reward as (
    SELECT
      date_trunc('month', date_start) as date_start,
      operator_name,
      SUM(token_amount) as reward_token,
      SUM(usd_amount) as reward_usd
    FROM chainlink_ethereum_ocr_reward_daily
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
    FROM chainlink_ethereum_ocr_gas_daily
    GROUP BY 1, 2
  ),
  request as (
    SELECT
      date_trunc('month', date_start) as date_start,
      operator_name,
      SUM(fulfilled_requests) as fulfilled_requests,
      SUM(reverted_requests) as reverted_requests,
      SUM(total_requests) as total_requests
    FROM chainlink_ethereum_ocr_request_daily
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