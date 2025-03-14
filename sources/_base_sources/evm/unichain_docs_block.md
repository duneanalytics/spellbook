{% docs unichain_transactions_doc %}

The `unichain.transactions` table contains detailed information about transactions on the Unichain blockchain. It includes:

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

{% docs unichain_traces_doc %}

The `unichain.traces` table contains records of execution steps for transactions on the Unichain blockchain. Each trace represents an atomic operation that modifies the blockchain state. Key components include:

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

{% docs unichain_traces_decoded_doc %}

The `unichain.traces_decoded` table contains decoded traces from verified smart contracts on the Unichain blockchain. It includes:

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

{% docs unichain_logs_doc %}

The `unichain.logs` table contains event logs emitted by smart contracts on the Unichain blockchain. It includes:

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

{% docs unichain_contracts_doc %}

The `unichain.contracts` table tracks verified smart contracts on the Unichain blockchain, including:

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

{% docs unichain_contracts_submitted_doc %}

The `unichain.contracts_submitted` table tracks contracts submitted for verification on the Unichain blockchain. It includes:

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

{% docs unichain_logs_decoded_doc %}

The `unichain.logs_decoded` table contains decoded event logs from verified smart contracts on the Unichain blockchain. It includes:

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

{% docs unichain_signatures_doc %}

The `unichain.signatures` table contains function and event signatures used for decoding contract interactions on the Unichain blockchain. It includes:

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

{% docs unichain_creation_traces_doc %}

The `unichain.creation_traces` table contains data about contract creation events on the Unichain blockchain. It includes:

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

{% docs unichain_blocks_doc %}

The `unichain.blocks` table contains information about blocks on the Unichain blockchain. It includes:

- Block identifiers: number, hash, time, date
- Gas metrics: gas_limit, gas_used
- Block characteristics: size, base_fee_per_gas
- Block roots: state_root, transactions_root, receipts_root
- Consensus data: difficulty, total_difficulty, nonce
- Block producer: miner
- Parent block: parent_hash
- Data availability: blob_gas_used, excess_blob_gas
- Beacon chain: parent_beacon_block_root

This table is fundamental for analyzing:
- Block production and timing
- Network capacity and usage
- Chain structure and growth
- Network performance metrics
- Validator/miner behavior
- Gas price dynamics
- Data availability layer metrics

{% enddocs %}

{% docs erc20_unichain_evt_transfer_doc %}

The `erc20_unichain.evt_transfer` table contains Transfer events emitted by ERC20 token contracts on the Unichain blockchain. Each record represents a token transfer between addresses. The table includes:

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

{% docs erc721_unichain_evt_transfer_doc %}

The `erc721_unichain.evt_transfer` table contains Transfer events emitted by ERC721 (NFT) contracts on the Unichain blockchain. Each record represents a unique token transfer between addresses. The table includes:

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

{% docs erc1155_unichain_evt_transfersingle_doc %}

The `erc1155_unichain.evt_transfersingle` table contains TransferSingle events emitted by ERC1155 contracts on the Unichain blockchain. Each record represents a single token type transfer between addresses. The table includes:

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

{% docs erc1155_unichain_evt_transferbatch_doc %}

The `erc1155_unichain.evt_transferbatch` table contains TransferBatch events emitted by ERC1155 contracts on the Unichain blockchain. Each record represents multiple token types being transferred between addresses in a single transaction. The table includes:

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
