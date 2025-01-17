{% docs corn_transactions_doc %}

The `corn.transactions` table contains detailed information about transactions on the Corn blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, from_address, to_address, value
- Gas data: gas_price, gas_limit, gas_used, max_fee_per_gas, priority_fee_per_gas
- Status: success or failure
- Input data for contract interactions
- Nonce and chain_id
- Transaction type and access list

This table is used for analyzing transaction patterns, gas usage, value transfers, and overall network activity on Corn.

{% enddocs %}

{% docs corn_traces_doc %}

The `corn.traces` table contains records of execution steps for transactions on the Corn blockchain. Each trace represents an atomic operation that modifies the state of the Ethereum Virtual Machine (EVM). Key components include:

- Transaction hash and block information
- From and to addresses
- Value transferred
- Gas metrics (gas, gas_used)
- Input and output data
- Call type (e.g., CALL, DELEGATECALL, CREATE)
- Error information and revert reasons
- Trace address for nested calls
- Sub-traces count

This table is essential for:
- Analyzing internal transactions
- Debugging smart contract interactions
- Tracking value flows through complex transactions
- Understanding contract creation and deployment

{% enddocs %}

{% docs corn_traces_decoded_doc %}

The `corn.traces_decoded` table contains a subset of decoded traces from the Corn blockchain dependent on submitted smart contracts and their ABIs. It includes:

- Block information and transaction details
- Contract name and namespace
- Decoded function names and signatures
- Trace address for execution path tracking
- Transaction origin and destination

This table is used for high level analysis of smart contract interactions. For fully decoded function calls and parameters, refer to protocol-specific decoded tables.

{% enddocs %}

{% docs corn_logs_doc %}

The `corn.logs` table contains event logs emitted by smart contracts on the Corn blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, index, from, to
- Contract address (emitting the event)
- Topic0 (event signature)
- Additional topics (indexed parameters)
- Data field (non-indexed parameters)
- Log index and transaction index

This table is crucial for:
- Tracking on-chain events
- Monitoring contract activity
- Analyzing token transfers
- Following protocol-specific events

{% enddocs %}

{% docs corn_logs_decoded_doc %}

The `corn.logs_decoded` table contains a subset of decoded logs from the Corn blockchain dependent on submitted smart contracts and their ABIs. It includes:

- Block and transaction information
- Contract details (name, namespace, address)
- Decoded event names and signatures
- Transaction origin and destination
- Event parameters (when available)

This table is used for high level analysis of smart contract events. For fully decoded events and parameters, refer to protocol-specific decoded tables.

{% enddocs %}

{% docs corn_blocks_doc %}

The `corn.blocks` table contains information about Corn blocks. It provides essential data about each block in the Corn blockchain, including:

- Block identifiers and timestamps
- Gas metrics and size
- Consensus information (difficulty, nonce)
- State roots and receipts
- Parent block information
- Blob gas information
- Parent beacon block root

This table is used for analyzing block production, network capacity, and chain state.

{% enddocs %}

{% docs corn_contracts_doc %}

The `corn.contracts` table contains information about verified smart contracts on the Corn blockchain. It includes:

- Contract address
- Contract bytecode
- Contract name and namespace
- Contract ABI
- Creation timestamp

This table is used for contract verification and analysis.

{% enddocs %}

{% docs corn_contracts_submitted_doc %}

The `corn.contracts_submitted` table tracks contracts submitted for decoding on the Corn blockchain. It includes:

- Contract address
- Contract name and namespace
- Submission details (timestamp, submitter)

This table is used for managing contract submissions and decoding status.

{% enddocs %}

{% docs corn_creation_traces_doc %}

The `corn.creation_traces` table contains information about contract creation events on the Corn blockchain. It includes:

- Block and transaction information
- Creator address
- Created contract address
- Contract bytecode
- Creation success status
- Gas consumption

This table is used for:
- Analyzing contract deployment patterns
- Tracking new contract deployments
- Understanding contract creation success rates

{% enddocs %}

{% docs erc20_corn_evt_transfer_doc %}

The `erc20_corn.evt_transfer` table contains Transfer events from ERC20 token contracts on the Corn blockchain. Each record represents a token transfer and includes:

- Token contract address
- Sender and recipient addresses
- Amount of tokens transferred
- Block and transaction information
- Event log details

This table is essential for:
- Tracking token transfers and holder activity
- Analyzing token distribution patterns
- Monitoring token holder behavior
- Calculating token balances
- Understanding token velocity and liquidity

{% enddocs %}

{% docs erc721_corn_evt_transfer_doc %}

The `erc721_corn.evt_transfer` table contains Transfer events from ERC721 (NFT) token contracts on the Corn blockchain. Each record represents an NFT transfer and includes:

- NFT contract address
- Token ID
- Sender and recipient addresses
- Block and transaction information
- Event log details

This table is used for:
- Tracking NFT ownership changes
- Analyzing NFT trading patterns
- Monitoring NFT collection activity
- Building NFT holder histories
- Understanding NFT market dynamics

{% enddocs %}

{% docs erc1155_corn_evt_transfer_doc %}

The `erc1155_corn.evt_transfersingle` and `erc1155_corn.evt_transferbatch` tables contain Transfer events from ERC1155 token contracts on the Corn blockchain. These tables track both fungible and non-fungible token transfers within the same contract. They include:

- Token contract address
- Token IDs
- Amounts transferred
- Sender, operator, and recipient addresses
- Block and transaction information
- Event log details

These tables are essential for:
- Tracking multi-token transfers
- Analyzing gaming asset movements
- Monitoring hybrid token systems
- Understanding complex token ecosystems
- Building token holder analytics

{% enddocs %}
