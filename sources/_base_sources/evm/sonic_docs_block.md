{% docs sonic_transactions_doc %}

The `sonic.transactions` table contains detailed information about transactions on the Sonic blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, from_address, to_address, value
- Gas data: gas_price, gas_limit, gas_used, max_fee_per_gas, priority_fee_per_gas
- Status: success or failure
- Input data for contract interactions
- Nonce and chain_id
- Transaction type and access list

This table is used for analyzing transaction patterns, gas usage, value transfers, and overall network activity on Sonic.

{% enddocs %}

{% docs sonic_traces_doc %}

The `sonic.traces` table contains records of execution steps for transactions on the Sonic blockchain. Each trace represents an atomic operation that modifies the state of the Ethereum Virtual Machine (EVM). Key components include:

- Transaction hash and block information
- From and to addresses
- Value transferred
- Gas metrics (gas, gas_used)
- Input and output data
- Call type (e.g., CALL, DELEGATECALL, CREATE)
- Error information and revert reasons
- Trace address for nested calls

This table is essential for:
- Analyzing internal transactions
- Debugging smart contract interactions
- Tracking value flows through complex transactions
- Understanding contract creation and deployment
- Monitoring protocol operations
- Analyzing cross-chain operations

{% enddocs %}

{% docs sonic_traces_decoded_doc %}

The `sonic.traces_decoded` table contains a subset of decoded traces from the Sonic blockchain dependent on submitted smart contracts and their ABIs. It includes:

- Block information and transaction details
- Contract name and namespace
- Decoded function names and signatures
- Trace address for execution path tracking
- Transaction origin and destination
- Function parameters (when available)

This table is used for high level analysis of smart contract interactions and protocol operations. For fully decoded function calls and parameters, refer to protocol-specific decoded tables.

{% enddocs %}

{% docs sonic_logs_doc %}

The `sonic.logs` table contains event logs emitted by smart contracts on the Sonic blockchain. It includes:

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
- Tracking cross-chain operations
- Monitoring protocol state changes

{% enddocs %}

{% docs sonic_logs_decoded_doc %}

The `sonic.logs_decoded` table contains a subset of decoded logs from the Sonic blockchain dependent on submitted smart contracts and their ABIs. It includes:

- Block and transaction information
- Contract details (name, namespace, address)
- Decoded event names and signatures
- Transaction origin and destination
- Event parameters (when available)

This table is used for high level analysis of smart contract events, particularly useful for monitoring protocol activities. For fully decoded events and parameters, refer to protocol-specific decoded tables.

{% enddocs %}

{% docs sonic_blocks_doc %}

The `sonic.blocks` table contains information about Sonic blocks. It provides essential data about each block in the Sonic blockchain, including:

- Block identifiers and timestamps
- Gas metrics and size
- Consensus information (difficulty, nonce)
- State roots and receipts
- Parent block information
- Base fee per gas
- Blob gas metrics
- Parent beacon block root

This table is fundamental for:
- Analyzing block production and timing
- Monitoring network performance
- Tracking gas usage patterns
- Understanding network upgrades
- Analyzing consensus metrics
- Studying blockchain structure

{% enddocs %}

{% docs sonic_creation_traces_doc %}

The `sonic.creation_traces` table contains information about contract deployments on the Sonic blockchain. It includes:

- Block information and timestamps
- Transaction details
- Contract addresses
- Creator addresses
- Contract bytecode

This table is essential for:
- Tracking smart contract deployments
- Analyzing contract creation patterns
- Monitoring new protocol deployments
- Auditing contract creation history
- Understanding contract deployment costs

{% enddocs %}

{% docs erc20_sonic_evt_transfer_doc %}

The `erc20_sonic.evt_transfer` table contains ERC20 token transfer events on the Sonic blockchain. Each record represents a token transfer and includes:

- Block information and timestamps
- Transaction details
- Token contract address
- Sender and receiver addresses
- Transfer amount
- Event index and transaction index

This table is essential for:
- Tracking token transfers and flows
- Analyzing token holder behavior
- Monitoring token activity
- Understanding token economics
- Tracking cross-chain token movements

{% enddocs %}

{% docs erc721_sonic_evt_transfer_doc %}

The `erc721_sonic.evt_transfer` table contains ERC721 (NFT) transfer events on the Sonic blockchain. Each record represents an NFT transfer and includes:

- Block information and timestamps
- Transaction details
- NFT contract address
- Sender and receiver addresses
- Token ID
- Event index and transaction index

This table is crucial for:
- Tracking NFT ownership changes
- Analyzing NFT trading patterns
- Monitoring NFT market activity
- Understanding NFT collections
- Tracking cross-chain NFT movements

{% enddocs %}

{% docs erc1155_sonic_evt_transfer_doc %}

The `erc1155_sonic.evt_transfer` table contains ERC1155 multi-token transfer events on the Sonic blockchain. Each record represents a token transfer and includes:

- Block information and timestamps
- Transaction details
- Token contract address
- Sender and receiver addresses
- Token ID and amount
- Event index and transaction index

This table is used for:
- Tracking multi-token transfers
- Analyzing token usage patterns
- Monitoring protocol activity
- Understanding multi-token operations
- Tracking DeFi integrations

{% enddocs %}

{% docs erc20_sonic_evt_approval_doc %}

The `erc20_sonic.evt_approval` table contains approval events for ERC20 tokens on the Sonic blockchain. Each record represents a token approval and includes:

- Block information and timestamps
- Transaction details
- Token contract address
- Owner address
- Spender address
- Approved amount
- Event index and transaction index

This table is essential for:
- Tracking token approvals
- Monitoring DeFi allowances
- Analyzing smart contract permissions
- Understanding protocol integrations
- Tracking liquidity provisions

{% enddocs %}

{% docs erc1155_sonic_evt_transfersingle_doc %}

The `erc1155_sonic.evt_transfersingle` table contains single transfer events for ERC1155 tokens on the Sonic blockchain. Each record represents a single token transfer and includes:

- Block information and timestamps
- Transaction details
- Token contract address
- Operator address
- Sender and receiver addresses
- Token ID and amount
- Event index and transaction index

This table is used for:
- Tracking individual token transfers
- Analyzing token usage patterns
- Monitoring protocol activity
- Understanding multi-token operations
- Tracking DeFi integrations

{% enddocs %}

{% docs erc1155_sonic_evt_transferbatch_doc %}

The `erc1155_sonic.evt_transferbatch` table contains batch transfer events for ERC1155 tokens on the Sonic blockchain. Each record represents a batch token transfer and includes:

- Block information and timestamps
- Transaction details
- Token contract address
- Operator address
- Sender and receiver addresses
- Array of token IDs and amounts
- Event index and transaction index

This table is crucial for:
- Tracking bulk token transfers
- Analyzing batch operations
- Monitoring large-scale movements
- Understanding protocol migrations
- Tracking multi-token operations

{% enddocs %}
