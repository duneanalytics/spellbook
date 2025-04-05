version: 2

models:
  - name: evaa_ton_withdraw
    meta:
      blockchain: ton
      sector: lending
      contributors: pshuvalov
    config:
      tags: ['ton', 'evaa', 'lending']
    description: >
      EVAA protocol withdraw events
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - tx_hash
            - block_date
    columns:
      - name: block_date
        description: "block_date of the transaction"
      - name: tx_hash
        description: "transaction hash"
      - name: trace_id
        description: "trace id"
      - name: tx_now
        description: "transaction timestamp"
      - name: tx_lt
        description: "transaction logical time"
      - name: pool_address
        description: "EVAA pool address"
      - name: pool_name
        description: "EVAA pool name"
      - name: owner_address
        description: "owner address"
      - name: sender_address
        description: "user smart contract address"
      - name: recipient_address
        description: "recipient address (only for after v4 protcool upgrade, otherwise null)"
      - name: asset_id
        description: "asset id"
      - name: withdraw_amount_current
        description: "withdraw amount"
      - name: user_new_principal
        description: "user new principal value"
      - name: new_total_supply
        description: "new total supply value"
      - name: new_total_borrow
        description: "new total borrow value"
      - name: s_rate
        description: "s rate value"
      - name: b_rate
        description: "b rate value"
  - name: evaa_ton_supply
    meta:
      blockchain: ton
      sector: lending
      contributors: pshuvalov
    config:
      tags: ['ton', 'evaa', 'lending']
    description: >
      EVAA protocol supply events
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - tx_hash
            - block_date
    columns:
      - name: block_date
        description: "block_date of the transaction"
      - name: tx_hash
        description: "transaction hash"
      - name: trace_id
        description: "trace id"
      - name: tx_now
        description: "transaction timestamp"
      - name: tx_lt
        description: "transaction logical time"
      - name: pool_address
        description: "EVAA pool address"
      - name: pool_name
        description: "EVAA pool name"
      - name: owner_address
        description: "owner address"
      - name: sender_address
        description: "user smart contract address"
      - name: asset_id
        description: "asset id"
      - name: amount_supplied
        description: "Amount supplied"
      - name: user_new_principal
        description: "user new principal value"
      - name: new_total_supply
        description: "new total supply value"
      - name: new_total_borrow
        description: "new total borrow value"
      - name: s_rate
        description: "s rate value"
      - name: b_rate
        description: "b rate value"
  - name: evaa_ton_liquidate
    meta:
      blockchain: ton
      sector: lending
      contributors: pshuvalov
    config:
      tags: ['ton', 'evaa', 'lending']
    description: >
      EVAA protocol liquidate events
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - tx_hash
            - block_date
    columns:
      - name: block_date
        description: "block_date of the transaction"
      - name: tx_hash
        description: "transaction hash"
      - name: trace_id
        description: "trace id"
      - name: tx_now
        description: "transaction timestamp"
      - name: tx_lt
        description: "transaction logical time"
      - name: pool_address
        description: "EVAA pool address"
      - name: pool_name
        description: "EVAA pool name"
      - name: owner_address
        description: "owner address"
      - name: sender_address
        description: "user smart contract address"
      - name: liquidator_address
        description: "liquidator address (only after v4 protocol upgrade, otherwise null)"
      - name: transferred_asset_id
        description: "transferred asset id"
      - name: transferred_amount
        description: "transferred amount"
      - name: new_user_loan_principal
        description: "New user loan principal value"
      - name: loan_new_total_supply
        description: "New loan total supply value"
      - name: loan_new_total_borrow
        description: "New loan total borrow value"
      - name: loan_s_rate
        description: "New loan s rate value"
      - name: loan_b_rate
        description: "New loan b rate value"
      - name: collateral_asset_id
        description: "Collateral asset id"
      - name: collateral_reward
        description: "Collateral reward value"
      - name: new_user_collateral_principal
        description: "New user collateral principal value"
      - name: new_collateral_total_supply
        description: "New collateral total supply value"
      - name: new_collateral_total_borrow
        description: "New collateral total borrow value"
      - name: collateral_s_rate
        description: "New collateral s rate value"
      - name: collateral_b_rate
        description: "New collateral b rate value"
  - name: evaa_ton_assets
    meta:
      blockchain: ton
      sector: lending
      contributors: pshuvalov
    config:
      tags: ['ton', 'evaa', 'lending']
    description: >
      EVAA protocol assets mapping
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - asset_id
    columns:
      - name: asset_id
        description: "asset id (uint256)"
      - name: asset_name
        description: "asset name"
      - name: jetton_master
        description: "jetton master address"
      - name: decimals
        description: "decimals"
