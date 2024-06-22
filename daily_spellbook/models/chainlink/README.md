# Price Feeds

## About

`chainlink.price_feeds*` models aggregate feed data from logs, add price answers, and truncate hourly and daily.

## Maintenance

The `oracle_addresses` and `oracle_token_mapping` models require periodic maintenance for adding or updating feeds. The process is currently manually but the LinkPool team is evaluating ways to automate this process. 

## Flow of Tables

- `price_feeds_oracle_addresses`: A meta file providing feed details based on the aggregator address.
- `price_feeds_oracle_token_mapping`: A meta file providing feed details based on the underlying token address.
- `price_feeds`: Selects logs filtered by an appropriate topic, joined by oracle_addresses. Adds a secondary join with token_mapping to get accurate price decimals. Truncated daily.
- `price_feeds_hourly`: `price_feeds` truncated hourly.


# OCR

## About

`chainlink.ocr*` models aggregate logs and transactions related to node operator gas usage, rewards, and requests and truncate daily. Ultimately, this is used to track financial performance, isolated by node operator and/or by network. Calculating OCR payments is inherently complex and requires intimate knowledge and understanding of the OCR payment system. This spellbook abstracts this complexity while still providing the ability for users to isolate at the lowest level (e.g., logs or transactions).

## Maintenance

The `ocr_operator_admin_meta` and `ocr_operator_node_meta` models require periodic maintenance for syncing with node details. The LinkPool team has built an automated internal tool that can be used to generate updated versions of these files periodically. Where possible, manual edits to these files should be avoided in favor of the automated generation script. Please mention @linkpool_ryan (gh: anon-r-7) or @linkpool_jon (gh: AnonJon) to request an update.

## Flow of Tables

### Node Operator Meta
- `ocr_operator_node_meta`: Node operators spend gas from each node on a given network. This is a catalogue of all current and historical node_addresses used by node operators.
- `ocr_operator_admin_meta`: Node operators receive payment to an `admin_address` per network for payment of all nodes run on that network. This is a catalogue of all current and historical admin_addresses used by node operators.
- *Note: Node meta is ultimately used to calculate gas costs by operators, and admin meta rewards. Both meta tables define a source-controlled `operator_name` which can be used to join gas and rewards to calculate margin* (e.g., `margin = (rewards - gas) / rewards`).

### Gas: Fulfilled Transactions
- `ocr_gas_transmission_logs`: Selects logs from the OCR gas event.
- `ocr_fulfilled_transactions`: Joins OCR logs with transactions and adds USD price to the closest minute.

### Gas: Reverted Transactions
- `ocr_reverted_transactions`: Isolates all failed transactions on nodes and adds USD price to the closest minute.

### Rewards
- `ocr_reward_transmission_logs`: Selects logs from the OCR reward event.
- `ocr_reward_evt_transfer`: Joins `reward_transmission_logs` to ERC20 event transfer to obtain token value for each log and injects operator details.
- `ocr_reward_evt_transfer_daily`: Truncates event transfers daily and calculates the sum of token value.

### Daily Summary
- `ocr_gas_daily`: Joins fulfilled and reverted transactions, truncates to daily calculating total gas in token and USD for each of fulfilled, reverted, and total, and injects operator details.
- `ocr_request_daily`: Joins fulfilled and reverted transactions, truncates to daily calculating the number of transactions in each of fulfilled, reverted, and total, and injects operator details.
- `ocr_reward_daily`: Distributes payments from `ocr_reward_evt_transfer_daily` over the number of days since the last payment (the closest methodology available to mirror intended payment schedules by OCR). Adds price USD per day and truncates results daily, including token and USD amounts with node operator details included.

## Usage Example

### Use Case A: Display OCR metrics for a given node operator on a monthly basis for the last N months

```sql
WITH 
  reward AS (
    SELECT
      date_month,
      operator_name,
      SUM(token_amount) AS reward_token,
      SUM(usd_amount) AS reward_usd
    FROM chainlink_ocr_reward_daily
    WHERE date_month >= CAST('2023-01-01' AS date)
    AND operator_name = 'LinkPool'
    GROUP BY 1, 2
  ),
  gas AS (
    SELECT
      date_month,
      operator_name,
      SUM(total_token_amount) AS gas_token,
      SUM(total_usd_amount) AS gas_usd
    FROM chainlink_ocr_gas_daily
    GROUP BY 1, 2
  ),
  request AS (
    SELECT
      date_month,
      operator_name,
      SUM(fulfilled_requests) AS fulfilled_requests,
      SUM(reverted_requests) AS reverted_requests,
      SUM(total_requests) AS total_requests
    FROM chainlink_ocr_request_daily
    GROUP BY 1, 2
  )
SELECT 
  reward.date_month,
  reward.operator_name,
  reward.reward_token,
  reward.reward_usd,
  gas.gas_token,
  gas.gas_usd,
  (reward.reward_usd - gas.gas_usd) AS margin_usd,
  (reward.reward_usd - gas.gas_usd) / reward.reward_usd AS margin_rate,
  request.fulfilled_requests,
  request.reverted_requests,
  request.total_requests,
  CAST(request.reverted_requests AS REAL) / request.total_requests AS revert_rate
FROM 
  reward
LEFT JOIN gas ON
  gas.date_month = reward.date_month AND
  gas.operator_name = reward.operator_name
LEFT JOIN request ON
  request.date_month = reward.date_month AND
  request.operator_name = reward.operator_name
ORDER BY 1, 2
