version: 2

models:
  - name: cow_protocol_plasma_solvers
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - address
    meta:
      blockchain: plasma
      project: cow_protocol
      contributors: harisang
    config:
      tags: ["plasma", "cow_protocol", "solver"]
    description: >
      CoW Protocol solvers list on plasma
  - name: cow_protocol_plasma_batches
    meta:
      blockchain: plasma
      project: cow_protocol
      contributors: harisang
    config:
      tags: ["plasma", "cow_protocol", "trades", "dex", "aggregator", "auction"]
    description: >
      CoW Protocol enriched batches table on Plasma
    data_tests:
      - unique:
          column_name: tx_hash

  - name: cow_protocol_plasma_eth_flow_orders
    meta:
      blockchain: plasma
      project: cow_protocol
      contributors: cowprotocol
    config:
      tags: [ "plasma", "cow_protocol", "eth-flow", "orders"]
    description: >
      ETHFlow enables the sale of Native ETH via CoW Protocol. This works essentially by placing an (onchain) order, 
      through the ETHFlow contract (https://github.com/cowprotocol/ethflowcontract) sending native which then wraps 
      the asset as an ERC20. The order is filled through this intermediary contract that uses ERC1271 signature 
      verification to place the order on the user's behalf.
