# Price Feeds

## About

chainlink.price_feeds* models aggregate feed data from logs, add prices answesr, and truncates hourly and daily.

## Flow of tables

- `price_feeds_oracle_addresses`: Meta file provides feed details based on aggregator address.
- `price_feeds_oracle_token_mapping`: Meta file provides feed details based on underlying token address
- `price_feeds`: Selected logs filtered by appropriate topic, joined by oracle_addresses. Adds secondary join with token_mapping to get accurate price decimals. Truncated daily.
- `price_feeds_hourly`: price_feeds. Trruncated hourly.

## Maintenance

These oracle_addresses and token_mapping models require consistent maintenance with adding feeds periodically. The LinkPool team has built an automated internal tool which can be used to periodically generate updated vesions of these files. Where possible, manual edits to these files should be avoided in favor of the automated generation script. Please mention @linkpool_ryan (gh: anon-r-7) or @linkpool_jon (gh: AnonJon) for request to update.

# OCR

## About

chainlink.ocr* models aggregate logs and transactions relating to node operator gas usage, rewards and requests and truncates daily. Ultimately, this is used to track financial performance, isolated by node operator and/or by network. Calculating OCR payments is inherently complex and requires intimate knowledge and understanding of the OCR payment system. This spellbook abstracts this complexity while still providing the ability for users to isolate even at the log or transaction level. 

## Flow of Tables

### Node Operator Meta
- `ocr_operator_node_meta`: Node operators spend gas from each node on a given network. This is a catalogue of all current and historical node_addresses used by node operators.
- `ocr_operator_admin_meta`: Node operators receive payment to an `admin_address` per network for payment of all nodes ran on that network. This is a catalogue of all current and historical admin_addresses used by node operators. 
- Node meta is ultimately used to calculate gas costs by operators, and admin meta rewards. Both meta tables define a source controlled `operator_name` which can be used to join gas and rewards to calaculate margin (e.g., `margin = (rewards - gas) / rewards`)

### Gas: Fulfilled Transactions
- `ocr_gas_transmission_logs`: Isolates logs to the OCR topic0 gas event
- `ocr_fulfilled_transactions`: Joins OCR logs with transactions and adds usd price to the closest minute

### Gas: Reverted Transactions
- `ocr_reverted_transactions`: Isolates all failed transactions on nodes and adds usd price to the closest minute

### Rewards
- `ocr_reward_transmission_logs`: Isolates logs to the OCR topic0 reward event
- `ocr_reward_evt_transfer`: Joins reward_transmission_logs to erc20 event transfer to obtain token value for each log and injects operator details
- `ocr_reward_evt_transfer_daily`: Truncates event transfers daily and calculate the sum of token value

### Daily Summary
- `ocr_gas_daily`: Joins fulfilled and reverted transactions, truncates to daily calculating total gas in token and usd for each of fulfilled, reverted, and total, and injects operator details
- `ocr_request_daily`: Joins fulfilled and reverted transactions, truncates to daily calculating number of transactions in each of fulfilled, reverted and total, and injects operator details
- `ocr_reward_daily`: Distributes payments from ocr_reward_evt_transfer_daily over the number of days since the last payment, the closest methodology available to mirror intended payment schedules by OCR. Adds price usd per day and truncates results to a token and usd amount of rewards per day with node operator details included. 

## Usage Example

### Use Case A: Display OCR metrics for a given node operator on a monthly basis for the last N months

```sql
WITH 
  reward as (
    SELECT
      date_trunc('month', date_start) as date_start,
      operator_name,
      SUM(token_amount) as reward_token,
      SUM(usd_amount) as reward_usd
    FROM chainlink_ethereum_ocr_reward_daily
    WHERE operator_name = '<some-operator>'
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