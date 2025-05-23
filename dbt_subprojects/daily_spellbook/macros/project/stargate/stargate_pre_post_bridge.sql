{% macro stargate_pre_post_bridge(blockchain) %}
WITH assets AS (
    SELECT * FROM {{ ref('stargate_bridge_token_mapping') }} WHERE chain = '{{ blockchain }}'
),
logs AS (
  SELECT
    e.block_time,
    e.block_number,
    e.contract_address AS pool_name,
    varbinary_substring(e.topic2, 13, 20) AS user,
    e.tx_hash,
    e.blockchain AS to_chain
  FROM {{ source(blockchain, 'logs') }} e
  JOIN assets a 
    ON e.contract_address = a.pool AND e.blockchain = a.chain
  WHERE topic0 = 0xefed6d3500546b29533b128a29e3a94d70788727f0507505ac12eaf2e578fd9c
    AND block_time > now() - interval '3' month
),
user_tx_counts AS (
  SELECT
    t.blockchain,
    t."from" AS user,
    COUNT(*) AS tx_count_last_30_days
  FROM {{ source(blockchain, 'transactions') }} t
  WHERE t.block_time >= CURRENT_DATE - INTERVAL '100' DAY
  GROUP BY t.blockchain, t."from"
),
eligible_users AS (
  SELECT DISTINCT q.user, q.to_chain AS blockchain
  FROM logs q
  JOIN user_tx_counts c ON q.user = c.user AND q.to_chain = c.blockchain
  WHERE c.tx_count_last_30_days <= 1500
),
input_tx AS (
  SELECT
    q.user,
    q.to_chain AS blockchain,
    q.tx_hash AS bridge_tx_hash,
    q.block_number AS bridge_block_number,
    q.block_time AS bridge_block_time
  FROM logs q
  JOIN eligible_users e ON q.user = e.user AND q.to_chain = e.blockchain
),
outgoing_transactions AS (
  SELECT
    t.blockchain,
    t."from" AS user,
    t.hash,
    t.block_number,
    t.index,
    t.block_time,
    t.data
  FROM {{ source(blockchain, 'transactions') }} t
  WHERE t.block_time >= CURRENT_DATE - INTERVAL '100' DAY
    AND (t.blockchain, t."from") IN (SELECT blockchain, user FROM eligible_users)
),
prev_tx AS (
  SELECT
    o.*,
    i.bridge_tx_hash,
    ROW_NUMBER() OVER (PARTITION BY i.bridge_tx_hash ORDER BY o.block_number DESC) AS rn
  FROM input_tx i
  JOIN outgoing_transactions o ON o.user = i.user AND o.blockchain = i.blockchain AND o.block_number < i.bridge_block_number
),
next_tx AS (
  SELECT
    o.*,
    i.bridge_tx_hash,
    ROW_NUMBER() OVER (PARTITION BY i.bridge_tx_hash ORDER BY o.block_number ASC) AS rn
  FROM input_tx i
  JOIN outgoing_transactions o ON o.user = i.user AND o.blockchain = i.blockchain AND o.block_number > i.bridge_block_number
),
pre_post AS (
  SELECT 
    i.user,
    i.blockchain,
    i.bridge_tx_hash,
    i.bridge_block_number,
    i.bridge_block_time,
    p.hash AS prev_tx_hash,
    p.block_number AS prev_block_number,
    p.block_time AS prev_block_time,
    p.data AS prev_data,
    n.hash AS next_tx_hash,
    n.block_number AS next_block_number,
    n.block_time AS next_block_time,
    n.data AS next_data
  FROM input_tx i
  LEFT JOIN prev_tx p ON p.bridge_tx_hash = i.bridge_tx_hash AND p.rn = 1
  LEFT JOIN next_tx n ON n.bridge_tx_hash = i.bridge_tx_hash AND n.rn = 1
)
SELECT DISTINCT * FROM pre_post
{% endmacro %}
