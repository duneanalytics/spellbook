{{
  config(
    
    alias='ccip_send_requested',
    materialized='view'
  )
}}

SELECT
  MAX(ccip_traces.blockchain) AS blockchain,
  MAX(ccip_traces.block_time) AS evt_block_time,
  MAX(reward_evt_transfer.value / 1e18) AS fee_amount,
  MAX(token_addresses.token_symbol) AS token,
  MAX(ccip_traces.chain_selector) as destination_chain_selector,
  MAX(ccip_traces.destination) as destination_blockchain
FROM
  {{ ref('chainlink_ethereum_ccip_send_traces') }} ccip_traces
  LEFT JOIN {{ source('erc20_ethereum', 'evt_Transfer') }} reward_evt_transfer ON reward_evt_transfer.evt_tx_hash = ccip_traces.tx_hash
  LEFT JOIN {{ ref('chainlink_ethereum_ccip_token_meta') }} token_addresses ON token_addresses.token_contract = reward_evt_transfer.contract_address
  LEFT JOIN {{ ref('chainlink_ethereum_ccip_tokens_locked_logs') }} tokens_locked ON tokens_locked.tx_hash = ccip_traces.tx_hash
WHERE
    (
        (
            ccip_traces.value > 0
            AND reward_evt_transfer.contract_address IN (
                SELECT
                    token_contract
                FROM
                    {{ ref('chainlink_ethereum_ccip_token_meta') }}
                WHERE
                    token_symbol = 'WETH'
            )
        )
        OR (
            ccip_traces.value <= 0
            AND reward_evt_transfer.contract_address IN (
                SELECT
                    token_contract
                FROM
                    {{ ref('chainlink_ethereum_ccip_token_meta') }}
                WHERE
                    token_symbol != 'WETH'
            )
        )
    )
    AND (
        tokens_locked.tx_hash IS NULL
        OR tokens_locked.total_tokens != reward_evt_transfer.value
    )
GROUP BY
  ccip_traces.tx_hash