{% docs ink_transactions_doc %}

The `ink.transactions` table contains detailed information about transactions on the Ink L2 blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, from_address, to_address, value
- L2 gas data: gas_price (lower than L1), gas_limit, gas_used
- L2 fee data: max_fee_per_gas, priority_fee_per_gas (optimized for L2)
- L1 data submission costs: l1_gas_used, l1_gas_price, l1_fee
- L1 reference data: l1_block_number, l1_timestamp (for cross-chain verification)
- Status: success or failure
- Input data for contract interactions
- Nonce (L2-specific sequence) and chain_id
- Transaction type and access list

This table is used for analyzing L2 transaction patterns, understanding L2 gas optimization, tracking L1-L2 cost relationships, and monitoring network activity.

{% enddocs %}

{% docs ink_traces_doc %}

The `ink.traces` table contains records of execution steps for transactions on the Ink L2 blockchain. Each trace represents an atomic operation within the L2 execution environment, which is optimized for faster and cheaper transactions while maintaining EVM compatibility. Key components include:

- Transaction hash and L2 block information
- From and to addresses (both L2 native and L1 bridged)
- Value transferred on L2
- L2 gas metrics (significantly lower than L1)
- Input and output data
- L2 call types (CALL, DELEGATECALL, CREATE)
- Error information and revert reasons
- Trace address for nested calls
- L1-L2 message passing information

This table is essential for:
- Analyzing internal transactions within the L2 network
- Debugging L2 smart contract interactions
- Tracking value flows through L2 transactions
- Understanding L2 contract deployments
- Monitoring cross-chain message passing
- Verifying L2 transaction execution

{% enddocs %}

{% docs ink_traces_decoded_doc %}

The `ink.traces_decoded` table contains decoded traces from the Ink L2 blockchain, focusing on verified smart contracts. It includes:

- L2 block information and transaction details
- Contract name and namespace
- Decoded function names and signatures
- L2 execution path tracking
- Transaction origin (L1 or L2) and destination
- Function parameters (when available)

This table is used for analyzing smart contract interactions on L2, with particular focus on cross-chain contract calls and L2-optimized protocols.

{% enddocs %}

{% docs ink_logs_doc %}

The `ink.logs` table contains event logs emitted by smart contracts on the Ink L2 blockchain. It includes:

- L2 block information: number, timestamp, hash
- L2 transaction details: hash, index, from, to
- Contract address (L2 native or L1 bridged)
- Topic0 (event signature)
- Additional topics (indexed parameters)
- Data field (non-indexed parameters)
- Log index and transaction index
- Cross-chain event information

This table is crucial for:
- Tracking L2-specific events
- Monitoring L2 contract activity
- Analyzing token transfers on L2
- Following protocol-specific events
- Understanding cross-chain interactions
- Verifying L1-L2 message processing

{% enddocs %}

{% docs ink_logs_decoded_doc %}

The `ink.logs_decoded` table contains decoded logs from verified smart contracts on the Ink L2 blockchain. It includes:

- L2 block and transaction information
- Contract details (name, namespace, L2 address)
- Decoded event names and signatures
- Transaction origin (L1 or L2) and destination
- Event parameters (when available)
- Cross-chain event context

This table is used for high-level analysis of L2 contract events, particularly focusing on cross-chain interactions and L2-specific protocol behavior.

{% enddocs %}

{% docs ink_blocks_doc %}

The `ink.blocks` table contains information about blocks on the Ink L2 blockchain. It provides essential data about each L2 block, including:

- L2 block identifiers and timestamps
- L2 gas metrics (optimized for lower costs)
- L2 block size and capacity
- State and transaction roots
- L2-specific sequencing data
- L1 reference information
- Blob gas metrics (for L2 data availability)
- Cross-chain synchronization data

This table is fundamental for analyzing:
- L2 block production and sequencing
- L2 network performance and capacity
- L1-L2 block relationships
- L2 state progression
- Data availability and compression
- Cross-chain finality guarantees

{% enddocs %}

{% docs ink_contracts_doc %}

The `ink.contracts` table tracks verified smart contracts on the Ink L2 blockchain, including:

- L2 contract address
- Contract bytecode (optimized for L2)
- Contract name and namespace
- Complete ABI
- L2 deployment timestamp
- Verification status
- L1 bridge relationship (if applicable)

This table is used for:
- L2 contract verification
- Cross-chain protocol analysis
- L2 development optimization
- Smart contract security on L2

{% enddocs %}

{% docs ink_contracts_submitted_doc %}

The `ink.contracts_submitted` table tracks contracts submitted for verification on the Ink L2 blockchain. It includes:

- L2 contract address
- Submission metadata
- Contract name and namespace
- Verification status
- L1 relationship status

This table helps track the progress of L2 contract verification and ecosystem development.

{% enddocs %}

{% docs ink_creation_traces_doc %}

The `ink.creation_traces` table contains data about contract creation events on the Ink L2 blockchain. It includes:

- L2 block information
- Transaction details
- Creator's address (L1 or L2)
- Created L2 contract address
- L2-optimized bytecode
- Creation status
- L2 gas consumption
- L1 data availability costs

This table is used for:
- Analyzing L2 contract deployment patterns
- Tracking L2 protocol launches
- Monitoring cross-chain contract deployment
- Understanding L2 deployment costs
- Optimizing contract creation

It's essentially a filtered version of the `ink.traces` table where `type = create`.

{% enddocs %}

{% docs erc20_ink_evt_transfer_doc %}

The `erc20_ink.evt_transfer` table contains Transfer events from ERC20 token contracts on the Ink L2 blockchain. Each record represents a token transfer and includes:

- L2 token contract address
- Sender and recipient addresses (L1 or L2)
- Token amount transferred
- L2 block and transaction information
- Cross-chain transfer flags
- Bridge interaction details (if applicable)

This table is essential for:
- Tracking L2 token transfers
- Analyzing token bridging patterns
- Monitoring L2 token activity
- Calculating L2 token balances
- Understanding L2 token economics
- Tracking cross-chain token flows

{% enddocs %}

{% docs erc721_ink_evt_transfer_doc %}

The `erc721_ink.evt_transfer` table contains Transfer events from ERC721 (NFT) token contracts on the Ink L2 blockchain. Each record represents an NFT transfer and includes:

- L2 NFT contract address
- Token ID
- Sender and recipient addresses (L1 or L2)
- L2 block and transaction information
- Bridge transfer information
- Cross-chain ownership data

This table is used for:
- Tracking L2 NFT movements
- Analyzing NFT bridging patterns
- Monitoring L2 NFT activity
- Building cross-chain NFT histories
- Understanding L2 NFT market dynamics
- Optimizing NFT operations on L2

{% enddocs %}
