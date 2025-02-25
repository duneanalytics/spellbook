{% docs sophon_blocks_doc %}
The `sophon.blocks` table contains information about blocks on the Sophon blockchain. Each row represents a single block.

**Main components:**
- Block identifiers: `number`, `hash`, `parent_hash`
- Gas metrics: `gas_limit`, `gas_used`, `base_fee_per_gas`, `blob_gas_used`, `excess_blob_gas`
- Block characteristics: `time`, `date`, `size`, `difficulty`, `total_difficulty`
- Block roots: `state_root`, `transactions_root`, `receipts_root`
- Consensus data: `nonce`, `parent_beacon_block_root`
- Block producer: `miner`

**Main use cases:**
- Analyzing block production and network capacity
- Monitoring gas usage and fee dynamics
- Tracking chain structure and consensus
- Measuring network performance metrics
{% enddocs %}

{% docs sophon_transactions_doc %}
The `sophon.transactions` table contains information about transactions on the Sophon blockchain. Each row represents a single transaction.

**Main components:**
- Transaction identifiers: `hash`, `block_number`, `block_time`, `block_date`, `index`
- Value transfer: `value`, `from`, `to`
- Gas and fees: `gas_limit`, `gas_used`, `gas_price`, `max_fee_per_gas`, `max_priority_fee_per_gas`, `base_fee_per_gas`, `effective_gas_price`
- Transaction data: `data`, `nonce`, `type`, `access_list`
- Status: `success`
- L1 data: `l1_block_number`, `l1_timestamp`, `l1_tx_origin`, `l1_gas_price`, `l1_gas_used`, `l1_fee`, `l1_fee_scalar`

**Main use cases:**
- Analyzing transaction patterns and user activity
- Monitoring gas costs and fee market dynamics
- Tracking value transfers and contract interactions
- Measuring L1-L2 interactions and costs
{% enddocs %}

{% docs sophon_traces_doc %}
The `sophon.traces` table contains execution traces of transactions on the Sophon blockchain. Each row represents a single trace within a transaction.

**Main components:**
- Trace identifiers: `tx_hash`, `trace_address`, `block_number`, `block_time`, `block_date`
- Value transfer: `value`, `from`, `to`
- Gas metrics: `gas`, `gas_used`
- Call data: `input`, `output`, `call_type`, `type`
- Status: `success`, `error`, `revert_reason`
- Contract creation: `address`, `code`

**Main use cases:**
- Analyzing internal contract calls
- Tracking contract creation and deployment
- Debugging failed transactions
- Monitoring complex contract interactions
{% enddocs %}

{% docs sophon_traces_decoded_doc %}
The `sophon.traces_decoded` table contains decoded execution traces from smart contracts on the Sophon blockchain. Each row represents a decoded function call.

**Main components:**
- Call identifiers: `tx_hash`, `trace_address`, `block_number`, `block_time`, `block_date`
- Contract info: `namespace`, `contract_name`
- Function data: `function_name`, `signature`
- Transaction context: `tx_from`, `tx_to`, `to`

**Main use cases:**
- Analyzing specific contract function calls
- Monitoring protocol activity
- Tracking cross-contract interactions
- Understanding contract usage patterns
{% enddocs %}

{% docs sophon_logs_doc %}
The `sophon.logs` table contains event logs emitted by smart contracts on the Sophon blockchain. Each row represents a single event log.

**Main components:**
- Log identifiers: `tx_hash`, `block_number`, `block_time`, `block_date`, `index`, `tx_index`
- Contract info: `contract_address`
- Event data: `topic0`, `topic1`, `topic2`, `topic3`, `data`
- Transaction context: `tx_from`, `tx_to`

**Main use cases:**
- Tracking contract events and state changes
- Monitoring token transfers and approvals
- Analyzing protocol activity
- Following contract-specific events
{% enddocs %}

{% docs sophon_logs_decoded_doc %}
The `sophon.logs_decoded` table contains decoded event logs from smart contracts on the Sophon blockchain. Each row represents a decoded event.

**Main components:**
- Event identifiers: `tx_hash`, `block_number`, `block_time`, `block_date`, `index`
- Contract info: `namespace`, `contract_name`, `contract_address`
- Event data: `event_name`, `signature`
- Transaction context: `tx_from`, `tx_to`

**Main use cases:**
- Analyzing specific contract events
- Monitoring protocol activity
- Tracking state changes
- Understanding contract behavior
{% enddocs %}

{% docs sophon_contracts_doc %}
The `sophon.contracts` table tracks decoded contracts on the Sophon blockchain. Each row represents a contract with its metadata.

**Main components:**
- Contract identifiers: `address`, `name`, `namespace`
- Creation info: `from`, `created_at`
- Code: `code`, `abi`, `abi_id`
- Flags: `dynamic`, `factory`, `sophon`
- Source: `detection_source`

**Main use cases:**
- Tracking contract deployments
- Monitoring protocol growth
- Analyzing contract relationships
- Supporting contract decoding
{% enddocs %}

{% docs sophon_creation_traces_doc %}
The `sophon.creation_traces` table contains information about contract creation events on the Sophon blockchain. Each row represents a contract deployment.

**Main components:**
- Creation identifiers: `tx_hash`, `block_number`, `block_time`, `block_month`
- Contract info: `address`, `from`, `code`

**Main use cases:**
- Tracking contract deployments
- Analyzing deployment patterns
- Monitoring protocol growth
- Understanding contract origins
{% enddocs %}

{% docs erc20_sophon_evt_transfer_doc %}
The `erc20_sophon.evt_transfer` table contains transfer events for ERC20 tokens on the Sophon blockchain. Each row represents a token transfer.

**Main components:**
- Event identifiers: `evt_tx_hash`, `evt_block_number`, `evt_block_time`, `evt_index`
- Transfer details: `from`, `to`, `value`
- Token info: `contract_address`

**Main use cases:**
- Tracking token transfers
- Analyzing token flows
- Monitoring user activity
- Measuring token adoption
{% enddocs %}

{% docs erc20_sophon_evt_approval_doc %}
The `erc20_sophon.evt_approval` table contains approval events for ERC20 tokens on the Sophon blockchain. Each row represents a token approval.

**Main components:**
- Event identifiers: `evt_tx_hash`, `evt_block_number`, `evt_block_time`, `evt_index`
- Approval details: `owner`, `spender`, `value`
- Token info: `contract_address`

**Main use cases:**
- Tracking token approvals
- Monitoring DEX activity
- Analyzing user permissions
- Understanding token usage
{% enddocs %}

{% docs erc1155_sophon_evt_transfersingle_doc %}
The `erc1155_sophon.evt_transfersingle` table contains single transfer events for ERC1155 tokens on the Sophon blockchain. Each row represents a token transfer.

**Main components:**
- Event identifiers: `evt_tx_hash`, `evt_block_number`, `evt_block_time`, `evt_index`
- Transfer details: `operator`, `from`, `to`, `id`, `value`
- Token info: `contract_address`

**Main use cases:**
- Tracking NFT transfers
- Analyzing NFT trading
- Monitoring collection activity
- Understanding NFT ownership
{% enddocs %}

{% docs erc1155_sophon_evt_transferbatch_doc %}
The `erc1155_sophon.evt_transferbatch` table contains batch transfer events for ERC1155 tokens on the Sophon blockchain. Each row represents a batch token transfer.

**Main components:**
- Event identifiers: `evt_tx_hash`, `evt_block_number`, `evt_block_time`, `evt_index`
- Transfer details: `operator`, `from`, `to`, `ids`, `values`
- Token info: `contract_address`

**Main use cases:**
- Tracking batch NFT transfers
- Analyzing NFT trading patterns
- Monitoring collection activity
- Understanding NFT ownership changes
{% enddocs %}

{% docs erc1155_sophon_evt_ApprovalForAll_doc %}
The `erc1155_sophon.evt_ApprovalForAll` table contains approval events for all tokens of an ERC1155 contract on the Sophon blockchain.

**Main components:**
- Event identifiers: `evt_tx_hash`, `evt_block_number`, `evt_block_time`, `evt_index`
- Approval details: `owner`, `operator`, `approved`
- Token info: `contract_address`

**Main use cases:**
- Tracking NFT approvals
- Monitoring marketplace permissions
- Analyzing operator authorizations
- Understanding NFT trading patterns
{% enddocs %}

{% docs erc721_sophon_evt_transfer_doc %}
The `erc721_sophon.evt_transfer` table contains transfer events for ERC721 tokens on the Sophon blockchain. Each row represents a token transfer.

**Main components:**
- Event identifiers: `evt_tx_hash`, `evt_block_number`, `evt_block_time`, `evt_index`
- Transfer details: `from`, `to`, `tokenId`
- Token info: `contract_address`

**Main use cases:**
- Tracking NFT transfers
- Analyzing NFT ownership changes
- Monitoring collection activity
- Understanding NFT trading patterns
{% enddocs %}

{% docs erc721_sophon_evt_Approval_doc %}
The `erc721_sophon.evt_Approval` table contains approval events for ERC721 tokens on the Sophon blockchain. Each row represents a token approval.

**Main components:**
- Event identifiers: `evt_tx_hash`, `evt_block_number`, `evt_block_time`, `evt_index`
- Approval details: `owner`, `approved`, `tokenId`
- Token info: `contract_address`

**Main use cases:**
- Tracking NFT approvals
- Monitoring marketplace permissions
- Analyzing trading patterns
- Understanding NFT permissions
{% enddocs %}

{% docs erc721_sophon_evt_ApprovalForAll_doc %}
The `erc721_sophon.evt_ApprovalForAll` table contains approval events for all tokens of an ERC721 contract on the Sophon blockchain.

**Main components:**
- Event identifiers: `evt_tx_hash`, `evt_block_number`, `evt_block_time`, `evt_index`
- Approval details: `owner`, `operator`, `approved`
- Token info: `contract_address`

**Main use cases:**
- Tracking NFT collection approvals
- Monitoring marketplace permissions
- Analyzing operator authorizations
- Understanding NFT trading patterns
{% enddocs %} 