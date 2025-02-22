{% docs sophon_transactions_doc %}

The `sophon.transactions` table contains detailed information about transactions on the Sophon blockchain. It includes:

- Block information: block_time, block_number, block_hash, block_date
- Transaction details: hash, from, to, value
- Gas metrics: gas_price, gas_limit, gas_used
- EIP-1559 fee parameters: max_fee_per_gas, max_priority_fee_per_gas, priority_fee_per_gas
- Transaction metadata: nonce, index, success
- Smart contract interaction: data
- Transaction type and access list
- Chain identification: chain_id
- L1 Layer data:
  - l1_gas_used: Gas consumed on L1
  - l1_gas_price: Gas price on L1
  - l1_fee: Total L1 fee
  - l1_fee_scalar: L1 fee calculation scalar
  - l1_block_number: Associated L1 block number
  - l1_timestamp: L1 block timestamp
  - l1_tx_origin: L1 transaction origin address

This table is used for:
- Analyzing transaction patterns and trends
- Monitoring network activity
- Studying gas fee variations
- Tracking L1-L2 interactions
- Evaluating network usage

{% enddocs %}

{% docs sophon_traces_doc %}

The `sophon.traces` table contains records of execution steps for transactions on the Sophon blockchain. Each trace represents an atomic operation that modifies the blockchain state. Key components include:

- Block information: block_time, block_number, block_hash, block_date
- Transaction context: tx_hash, tx_index, tx_from, tx_to
- Value transfer details
- Gas metrics: gas, gas_used
- Input and output data
- Call type (CALL, DELEGATECALL, CREATE)
- Error information and revert reasons
- Trace address for nested calls
- Contract creation data: address, code

This table is essential for:
- Analyzing internal transactions
- Debugging smart contract interactions
- Tracking value flows through complex transactions
- Understanding contract creation and deployment
- Monitoring inter-contract calls

{% enddocs %}

{% docs sophon_traces_decoded_doc %}

The `sophon.traces_decoded` table contains decoded traces from verified smart contracts on the Sophon blockchain. It includes:

- Block information: block_date, block_time, block_number
- Contract context: namespace, contract_name
- Transaction details: tx_hash, tx_from, tx_to
- Execution path: trace_address
- Function identification: signature, function_name

This table is used for:
- Analyzing specific function calls in smart contracts
- Monitoring contract usage patterns
- Tracking protocol-level on-chain activity
- Studying inter-protocol interactions
- Auditing smart contract behavior

{% enddocs %}

{% docs sophon_logs_doc %}

The `sophon.logs` table contains event logs emitted by smart contracts on the Sophon blockchain. It includes:

- Block information: block_time, block_number, block_hash, block_date
- Transaction details: tx_hash, tx_index, tx_from, tx_to
- Contract address
- Event topics:
  - topic0 (event signature)
  - topic1 (first indexed parameter)
  - topic2 (second indexed parameter)
  - topic3 (third indexed parameter)
- Event data
- Log position: index

This table is crucial for:
- Tracking on-chain events
- Monitoring contract activity
- Analyzing token transfers
- Following protocol-specific events
- Building event-driven applications
- Studying cross-protocol interactions

Key use cases:
1. DeFi protocol analysis
2. NFT transaction tracking
3. Governance event monitoring
4. Bridge activity analysis
5. User behavior research

{% enddocs %}

{% docs sophon_contracts_doc %}

The `sophon.contracts` table tracks verified smart contracts on the Sophon blockchain, including:

- Contract identification: address, name, namespace
- Contract code and interface: abi, code
- Contract metadata: 
  - dynamic: indicates dynamically created contracts
  - base: indicates base contracts
  - factory: indicates factory contracts
- Creation information: from, created_at
- Verification details: detection_source

This table is used for:
- Contract verification and analysis
- Protocol research and monitoring
- Development and debugging
- Smart contract security analysis
- Understanding contract relationships and hierarchies

{% enddocs %}

{% docs sophon_contracts_submitted_doc %}

The `sophon.contracts_submitted` table tracks contracts submitted for verification on the Sophon blockchain. It includes:

- Contract address and identification
- Submission metadata (timestamp, submitter)
- Contract name and namespace
- Contract code and ABI
- Contract type flags (dynamic, factory)

This table helps track:
- Contract verification progress
- Community contributions
- Contract deployment patterns
- Protocol development activity
- Smart contract ecosystem growth

{% enddocs %}

{% docs sophon_logs_decoded_doc %}

The `sophon.logs_decoded` table contains decoded event logs from verified smart contracts on the Sophon blockchain. It includes:

- Block information: block_date, block_time, block_number
- Contract details: namespace, contract_name, contract_address
- Transaction context: tx_hash, tx_from, tx_to
- Event identification: signature, event_name
- Log position: index

This table is essential for:
- Analyzing smart contract events with decoded data
- Monitoring protocol activities
- Tracking specific contract events
- Building event-driven analytics
- Understanding protocol interactions

{% enddocs %}

{% docs sophon_signatures_doc %}

The `sophon.signatures` table contains function and event signatures used for decoding contract interactions on the Sophon blockchain. It includes:

- Signature identification: id, signature
- Function/Event details: name, type
- ABI information
- Namespace association
- Creation timestamp

This table is used for:
- Contract interaction decoding
- Function and event identification
- Protocol analysis
- Smart contract debugging
- Cross-protocol interaction analysis

{% enddocs %}

{% docs sophon_creation_traces_doc %}

The `sophon.creation_traces` table contains data about contract creation events on the Sophon blockchain. It includes:

- Block information: block_time, block_number, block_month
- Transaction details: tx_hash
- Contract details: address, from, code

This table is used for:
- Analyzing contract deployment patterns
- Tracking smart contract origins
- Monitoring protocol deployments
- Understanding contract creation
- Studying factory contract behavior

{% enddocs %}

{% docs erc20_sophon_evt_transfer_doc %}

The `erc20_sophon.evt_transfer` table contains Transfer events emitted by ERC20 token contracts on the Sophon blockchain. Each record represents a token transfer between addresses. The table includes:

- Contract information: contract_address
- Event context: evt_tx_hash, evt_index, evt_block_time, evt_block_number
- Transfer details:
  - from: Sender address
  - to: Recipient address
  - value: Amount of tokens transferred

This table is essential for:
- Tracking token transfers and flows
- Analyzing token holder behavior
- Monitoring trading activity
- Calculating token balances
- Studying token distribution patterns

{% enddocs %}

{% docs erc721_sophon_evt_transfer_doc %}

The `erc721_sophon.evt_transfer` table contains Transfer events emitted by ERC721 (NFT) contracts on the Sophon blockchain. Each record represents a unique token transfer between addresses. The table includes:

- Contract information: contract_address
- Event context: evt_tx_hash, evt_index, evt_block_time, evt_block_number
- Transfer details:
  - from: Previous owner's address
  - to: New owner's address
  - tokenId: Unique identifier of the NFT

This table is used for:
- Tracking NFT ownership changes
- Analyzing NFT trading patterns
- Monitoring collection activity
- Studying NFT market dynamics
- Building NFT provenance histories

{% enddocs %}

{% docs erc1155_sophon_evt_transfersingle_doc %}

The `erc1155_sophon.evt_transfersingle` table contains TransferSingle events emitted by ERC1155 contracts on the Sophon blockchain. Each record represents a single token type transfer between addresses. The table includes:

- Contract information: contract_address
- Event context: evt_tx_hash, evt_index, evt_block_time, evt_block_number
- Transfer details:
  - operator: Address approved to make the transfer
  - from: Sender address
  - to: Recipient address
  - id: Token type identifier
  - value: Amount of tokens transferred

This table is useful for:
- Tracking multi-token transfers
- Analyzing gaming asset movements
- Monitoring fungible token flows
- Studying semi-fungible token usage
- Building token inventory systems

{% enddocs %}

{% docs erc1155_sophon_evt_transferbatch_doc %}

The `erc1155_sophon.evt_transferbatch` table contains TransferBatch events emitted by ERC1155 contracts on the Sophon blockchain. Each record represents multiple token types being transferred between addresses in a single transaction. The table includes:

- Contract information: contract_address
- Event context: evt_tx_hash, evt_index, evt_block_time, evt_block_number
- Transfer details:
  - operator: Address approved to make the transfer
  - from: Sender address
  - to: Recipient address
  - ids: Array of token type identifiers
  - values: Array of amounts transferred for each token type

This table is crucial for:
- Tracking bulk token transfers
- Analyzing batch operations
- Monitoring complex token movements
- Studying multi-token transactions
- Building efficient token tracking systems

{% enddocs %}

{% docs erc20_sophon_evt_approval_doc %}

The `erc20_sophon.evt_approval` table contains Approval events for ERC20 tokens on the Sophon blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and spender addresses
- Approved amount

This table is used for analyzing ERC20 token approvals and spending permissions on the Sophon network.

{% enddocs %}

{% docs erc721_sophon_evt_approval_doc %}

The `erc721_sophon.evt_approval` table contains Approval events for ERC721 tokens on the Sophon blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and approved addresses
- Token ID

This table is used for analyzing approvals for individual ERC721 tokens (NFTs) on the Sophon network.

{% enddocs %}

{% docs erc721_sophon_evt_approvalforall_doc %}

The `erc721_sophon.evt_approvalforall` table contains ApprovalForAll events for ERC721 tokens on the Sophon blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC721 token collections on the Sophon network.

{% enddocs %}

{% docs erc1155_sophon_evt_approvalforall_doc %}

The `erc1155_sophon.evt_approvalforall` table contains ApprovalForAll events for ERC1155 tokens on the Sophon blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Account and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC1155 token collections on the Sophon network.

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