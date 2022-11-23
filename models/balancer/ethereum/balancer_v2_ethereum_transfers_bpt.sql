{{
    config(
        alias='transfers_bpt',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["stefenon"]\') }}'
    )
}}

WITH registered_pools AS (
    SELECT
      DISTINCT poolAddress AS pool_address
    FROM
      {{ source('balancer_v2_ethereum', 'Vault_evt_PoolRegistered') }}
  )

SELECT DISTINCT * FROM (
    SELECT
        logs.contract_address,
        logs.tx_hash AS evt_tx_hash,
        logs.tx_index AS evt_index,
        logs.block_time AS evt_block_time,
        logs.block_number AS evt_block_number,
        CONCAT('0x', SUBSTRING(logs.topic2, 27, 40)) AS from,
        CONCAT('0x', SUBSTRING(logs.topic3, 27, 40)) AS to,
        bytea2numeric(SUBSTRING(logs.data, 32, 64)) AS value
    FROM {{ source('ethereum', 'logs') }} logs
    INNER JOIN registered_pools p ON p.pool_address = logs.contract_address
    WHERE logs.topic1 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    AND logs.block_number >= 12274556 ) transfers
