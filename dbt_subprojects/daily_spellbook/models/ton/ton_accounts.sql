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

-- list of interfaces with their name and link to source code
-- Names convention: lowercase, no spaces, underscores
-- Starts with interface category like 'wallet_' (all user walles), 'validation_' (all contracts related to validators), 'nft_' (all nft contracts),
-- 'jetton_' (all jetton contracts), etc
WITH INTERFACES AS (
  SELECT code_hash, interface, link FROM (VALUES
      ('/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=', 'wallet_v4r2', 'https://github.com/ton-blockchain/wallet-contract'),
      ('IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8=', 'wallet_v5r1', 'https://github.com/ton-blockchain/wallet-contract-v5'),
      ('thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=', 'wallet_v3r1', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/wallet3-code.fc'),
      ('hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=', 'wallet_v3r2', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/wallet3-code.fc'),
      ('89fKU0k97trCizgZhqhJQDy6w9LFhHea8IEGWvCsS5M=', 'wallet_v5_beta', 'https://github.com/ton-blockchain/wallet-contract-v5'),
      ('/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=', 'wallet_v2r2', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/wallet-code.fc'),
      ('ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=', 'wallet_v4r1', 'https://github.com/ton-blockchain/wallet-contract'),
      ('MZrVLsmoWWIPil2Ww2CJ5nw29OOTAdBQ224VCXAZzpE=', 'wallet_v5_beta', 'https://github.com/ton-blockchain/wallet-contract-v5'),
      ('WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=', 'wallet_v1r3', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/new-wallet.fif'),
      ('XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=', 'wallet_v2r1', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/wallet-code.fc'),
      ('oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=', 'wallet_v1r1', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/new-wallet.fif'),
      ('1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=', 'wallet_v1r2', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/new-wallet.fif'),
      ('EayteVWEQJDyg78ji8FEmHH3g+fMCXlAjT9IWUg+hSU=', 'wallet_highload_v3r1', 'https://github.com/ton-blockchain/highload-wallet-contract-v3'),
      ('ID3U81ittJmTEpqpJcrDmRa2ig5PeNJujywraer6Vnk=', 'wallet_highload_v2r2', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/highload-wallet-v2-code.fc'),
      ('lJTRzI7fEvBWcaGpugmSEJbrUIEeGSTsZcPGKfu4CBI=', 'wallet_highload_v2', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/highload-wallet-v2-code.fc'),
      ('gwyZpEfQl0222G7X2J/k8tLsIjWMtkKH4tMeVj695Uc=', 'locker', 'https://github.com/ton-blockchain/locker-contract/blob/main/contracts/locker.fc'),
      ('Yhf4csmfr8uHDywRo2L1kzm+lQlfcNALnP8vbc1p090=', 'elector', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/elector-code.fc'),
      ('09FNqaYn8Ow1MzQYKXYq+SuVQLIb8DZl+sCcK0bqu6w=', 'multisig_v2', 'https://github.com/ton-blockchain/multisig-contract-v2'),
      ('tItTGr7DtxRjgpH3137W3J9qJynvyiBHcTc3TUrotZA=', 'vesting_wallet', 'https://github.com/ton-blockchain/vesting-contract/blob/main/contracts/vesting_wallet.fc'),
      ('zA05WJ6ywM/g/eKEVmV6O909lTlVrj+Y8lZkqzyQT70=', 'validation_single_nominator', 'https://github.com/orbs-network/single-nominator/blob/main/contracts/single-nominator.fc'),
      ('mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8=', 'validation_nominator_pool', 'https://github.com/ton-blockchain/nominator-pool/blob/main/func/pool.fc'),
      ('k1qO5V0KFi7JP/LZDv0vGceiMBSqsjFYiWbcO4Oyq48=', 'multisig', 'https://github.com/ton-blockchain/ton/blob/master/crypto/smartcont/multisig-code.fc'),
      ('pCrmnqx2/+DkUtPU8T04ehTkbAGlqtul/B2JPmxx9bo=', 'validation_single_nominator', 'https://github.com/orbs-network/single-nominator/blob/main/contracts/single-nominator.fc'),
      ('jq1XzZOrnu/v3TgHSWnM611b7o1JcbnLPsW1fQ7K9mM=', 'multisig', 'https://github.com/ton-blockchain/multisig-contract/blob/master/multisig-code.fc'),
      ('XWJnptWjJbEHXAuoChKkoDx/T2dO6vKO5BZd9TUuV40=', 'validation_whales_pool', 'https://github.com/tonwhales/ton-nominators'),
      ('p6Jhak1jmgdsL2fnzOBCP9Khwu5VCtZRwe2hbuE7yso=', 'nft_telemint_item', 'https://github.com/TelegramMessenger/telemint'),
      ('WwkpkWUPvEizKIsIrMdneyLkwl1IRYxW0qF/I+gGtrQ=', 'nft_telemint_item', 'https://github.com/TelegramMessenger/telemint'),
      ('MNzX1bb89ZaMJ5Hh4xrXMvJnLymG4daXDrA8yiLSMlE=', 'nft_telemint_item', 'https://github.com/TelegramMessenger/telemint'),
      ('i3EeaAlnLAM13GHR2h3MQFnJcySK9we9WGJFCF6nCzI=', 'nft_telemint_item', 'https://github.com/TelegramMessenger/telemint')
  ) AS t(code_hash, interface, link)
),
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
LEFT JOIN INTERFACES I on T.code_hash = I.code_hash