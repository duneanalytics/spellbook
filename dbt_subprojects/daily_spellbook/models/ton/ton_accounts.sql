{{
    config(
        schema = 'ton',
        alias='accounts',
        
        materialized = 'table',
        unique_key = ['address'],
        post_hook='{{ expose_spells(\'["ton"]\',
                                    "sector",
                                    "ton",
                                    \'["pshuvalov"]\') }}'
    )
}}



WITH 
TON_STATES AS (
    SELECT 
        account AS address,
        MAX_BY(end_status, lt) status,
        MAX(block_time) last_tx_at,
        MAX_BY(hash, lt) last_tx_hash,
        MAX_BY(account_state_balance_after, lt) FILTER (WHERE account_state_balance_after IS NOT NULL) balance,
        MAX_BY(account_state_code_hash_after, lt) FILTER (WHERE account_state_code_hash_after IS NOT NULL) code_hash,

        MAX_BY(hash, lt) FILTER (WHERE end_status = 'active' AND orig_status != 'active') deployment_tx_hash,
        MAX_BY(block_time, lt) FILTER (WHERE end_status = 'active' AND orig_status != 'active') deployment_at,
        MAX_BY(hash = trace_id, lt) FILTER (WHERE end_status = 'active' AND orig_status != 'active') deployment_by_external,

        MIN_BY(hash, lt) initial_funding_tx_hash,
        MIN_BY(block_time, lt) initial_funding_at,
        MIN_BY(block_date, lt) initial_funding_date
    FROM {{ source('ton', 'transactions') }} T
    WHERE 1=1
    GROUP BY 1
), JETTON_WALLETS AS (
  SELECT DISTINCT jetton_wallet, 'jetton_wallet' AS interface  FROM {{ source('ton', 'jetton_events') }}
), JETTON_MASTERS AS (
  SELECT DISTINCT jetton_master, 'jetton_master' AS interface FROM {{ source('ton', 'jetton_events') }}
), DEX_POOLS AS (
  SELECT DISTINCT pool_address, 'dex_pool' AS interface FROM {{ source('ton', 'dex_trades') }}
), DEX_ROUTERS AS (
  SELECT DISTINCT router_address, 'dex_router' AS interface FROM {{ source('ton', 'dex_trades') }}
), UNIQUE_CODE_ACCOUNTS AS (
  -- To avoid situation when some jetton wallet is updated to the code_hash of an existing contract of a different type
  SELECT account, MAX(account_state_code_hash_after) as code_hash
  FROM {{ source('ton', 'transactions') }} T
  WHERE account_state_code_hash_after IS NOT NULL
  GROUP BY 1
  HAVING COUNT(DISTINCT account_state_code_hash_after) = 1
), JETTON_RELATED_CODE_HASHES AS (
  SELECT code_hash,
  ARRAY_DISTINCT(ARRAY_AGG(I.interface)) as interfaces
  FROM UNIQUE_CODE_ACCOUNTS T
  LEFT JOIN JETTON_WALLETS JW ON JW.jetton_wallet = account
  LEFT JOIN JETTON_MASTERS JM ON JM.jetton_master = account
  LEFT JOIN DEX_POOLS DP ON DP.pool_address = account
  LEFT JOIN DEX_ROUTERS DR ON DR.router_address = account
  CROSS JOIN UNNEST(ARRAY[JW.interface, JM.interface, DP.interface, DR.interface]) AS I(interface)
  WHERE I.interface IS NOT NULL
  GROUP BY 1
)
SELECT T.address, T.status, T.last_tx_hash, T.last_tx_at, T.balance, T.code_hash, T.deployment_tx_hash, T.deployment_at, T.deployment_by_external,
T.initial_funding_tx_hash, T.initial_funding_at, M.source AS first_tx_sender,
array_union(COALESCE(J.interfaces, ARRAY[]), CASE WHEN I.interface IS NOT NULL THEN ARRAY[I.interface] ELSE ARRAY[] END) AS interfaces FROM TON_STATES T 
LEFT JOIN {{ source('ton', 'messages') }} M ON T.initial_funding_tx_hash = M.tx_hash and M.direction ='in' and M.block_date = T.initial_funding_date
LEFT JOIN JETTON_RELATED_CODE_HASHES J on T.code_hash = J.code_hash
LEFT JOIN {{ ref('ton_interfaces_by_code_hash_seed') }} I on T.code_hash = I.code_hash