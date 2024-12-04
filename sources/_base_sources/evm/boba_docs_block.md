{% docs boba_transactions_doc %}

The `boba.transactions` table contains detailed information about transactions on the Boba blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, from_address, to_address, value
- Gas data: gas_price, gas_limit, gas_used, max_fee_per_gas, priority_fee_per_gas
- L1 data: l1_gas_used, l1_gas_price, l1_fee, l1_block_number, l1_timestamp
- Status: success or failure
- Input data for contract interactions
- Nonce and chain_id
- Transaction type and access list

This table is used for analyzing transaction patterns, gas usage, value transfers, L1-L2 interactions, and overall network activity on Boba.

{% enddocs %}

{% docs boba_traces_doc %}

The `boba.traces` table contains records of execution steps for transactions on the Boba blockchain. Each trace represents an atomic operation that modifies the state of the Ethereum Virtual Machine (EVM). Key components include:

- Transaction hash and block information
- From and to addresses
- Value transferred
- Gas metrics (gas, gas_used)
- Input and output data
- Call type (e.g., CALL, DELEGATECALL, CREATE)
- Error information and revert reasons
- Trace address for nested calls
- L1-L2 specific information

This table is essential for:
- Analyzing internal transactions
- Debugging smart contract interactions
- Tracking value flows through complex transactions
- Understanding contract creation and deployment
- Monitoring L1-L2 message passing

{% enddocs %}

{% docs boba_traces_decoded_doc %}

The `boba.traces_decoded` table contains a subset of decoded traces from the Boba blockchain dependent on submitted smart contracts and their ABIs. It includes:

- Block information and transaction details
- Contract name and namespace
- Decoded function names and signatures
- Trace address for execution path tracking
- Transaction origin and destination
- Function parameters (when available)

This table is used for high level analysis of smart contract interactions. For fully decoded function calls and parameters, refer to protocol-specific decoded tables.

{% enddocs %}

{% docs boba_logs_doc %}

The `boba.logs` table contains event logs emitted by smart contracts on the Boba blockchain. It includes:

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
- Understanding L1-L2 interactions

{% enddocs %}

{% docs boba_logs_decoded_doc %}

The `boba.logs_decoded` table contains a subset of decoded logs from the Boba blockchain dependent on submitted smart contracts and their ABIs. It includes:

- Block and transaction information
- Contract details (name, namespace, address)
- Decoded event names and signatures
- Transaction origin and destination
- Event parameters (when available)

This table is used for high level analysis of smart contract events. For fully decoded events and parameters, refer to protocol-specific decoded tables.

{% enddocs %}

{% docs boba_blocks_doc %}

The `boba.blocks` table contains information about Boba blocks. It provides essential data about each block in the Boba blockchain, including:

- Block identifiers and timestamps
- Gas metrics and size
- Consensus information (difficulty, nonce)
- State roots and receipts
- L2-specific block data
- Parent block information

This table is fundamental for analyzing:
- Blockchain structure and growth
- Block production rates
- Network performance
- L1-L2 block relationships
- Chain reorganizations

{% enddocs %}

{% docs boba_contracts_doc %}

The `boba.contracts` table tracks decoded contracts on Boba, including:

- Contract address and bytecode
- Contract name and namespace
- Complete ABI
- Creation timestamp
- Verification status

This table is used for:
- Contract verification and analysis
- Protocol research and monitoring
- Development and debugging
- Smart contract security analysis

{% enddocs %}

{% docs boba_contracts_submitted_doc %}

The `boba.contracts_submitted` table tracks contracts submitted for decoding on Boba. It includes:

- Contract address
- Submission metadata (timestamp, submitter)
- Contract name and namespace
- Verification status

This table helps track the progress of contract verification and community contributions to the Boba ecosystem.

{% enddocs %}

{% docs boba_creation_traces_doc %}

The `boba.creation_traces` table contains data about contract creation events on the Boba blockchain. It includes:

- Block information and timestamps
- Transaction details
- Creator's address
- Created contract address
- Deployed contract bytecode
- Creation success status
- Gas consumption

This table is used for:
- Analyzing contract deployment patterns
- Tracking smart contract origins
- Monitoring protocol deployments
- Understanding contract creation costs

It's essentially a filtered version of the `boba.traces` table where `type = create`.

{% enddocs %}
