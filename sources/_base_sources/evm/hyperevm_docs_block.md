{% docs hyperevm_blocks_doc %}

The `hyperevm.blocks` table contains information about blocks on the hyperevm blockchain. It includes:

- Block identifiers: number, hash, time, date
- Gas metrics: gas_limit, gas_used, blob_gas_used, excess_blob_gas
- Block characteristics: size, base_fee_per_gas
- Block roots: state_root, transactions_root, receipts_root, parent_beacon_block_root
- Block producer: miner
- Parent block: parent_hash

This table is fundamental for analyzing:
- Block production and timing
- Network capacity and usage
- Chain structure and growth
- Network performance metrics
- Blob gas usage patterns

{% enddocs %}

{% docs hyperevm_transactions_doc %}

The `hyperevm.transactions` table contains detailed information about transactions on the hyperevm blockchain. It includes:

- Block information: block_time, block_number, block_hash, block_date
- Transaction details: hash, from, to, value
- Gas metrics: gas_price, gas_limit, gas_used
- EIP-1559 fee parameters: max_fee_per_gas, max_priority_fee_per_gas, priority_fee_per_gas
- Transaction metadata: nonce, index, success
- Smart contract interaction: data
- Transaction type and access list
- Chain identification: chain_id
 

This table is used for analyzing:
- Transaction patterns and volume
- Gas usage and fee trends
- Smart contract interactions
- Network activity and usage
 

{% enddocs %}

{% docs hyperevm_logs_doc %}

The `hyperevm.logs` table contains event logs emitted by smart contracts on the hyperevm blockchain. It includes:

- Block information: block_time, block_number, block_hash, block_date
- Transaction details: tx_hash, tx_index, tx_from, tx_to
- Contract address
- Event topics: topic0 (event signature), topic1, topic2, topic3
- Event data
- Log position: index

This table is crucial for:
- Tracking on-chain events
- Monitoring contract activity
- Analyzing token transfers
- Following protocol-specific events
- Understanding smart contract interactions

{% enddocs %}


