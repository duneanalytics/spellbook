{% docs flare_transactions_doc %}

The `flare.transactions` table contains detailed information about transactions on the Flare blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, from_address, to_address, value
- Gas data: gas_price, gas_limit, gas_used, max_fee_per_gas, priority_fee_per_gas
- Status: success or failure
- Input data for contract interactions
- Nonce and chain_id
- Transaction type and access list

This table is used for analyzing transaction patterns, gas usage, value transfers, and overall network activity on Flare.

{% enddocs %}

{% docs flare_traces_doc %}

The `flare.traces` table contains records of execution steps for transactions on the Flare blockchain. Each trace represents an atomic operation that modifies the state of the Ethereum Virtual Machine (EVM). Key components include:

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
- Monitoring FTSO (Flare Time Series Oracle) interactions
- Analyzing State Connector operations

{% enddocs %}

{% docs flare_traces_decoded_doc %}

The `flare.traces_decoded` table contains a subset of decoded traces from the Flare blockchain dependent on submitted smart contracts and their ABIs. It includes:

- Block information and transaction details
- Contract name and namespace
- Decoded function names and signatures
- Trace address for execution path tracking
- Transaction origin and destination
- Function parameters (when available)

This table is used for high level analysis of smart contract interactions, including FTSO and State Connector operations. For fully decoded function calls and parameters, refer to protocol-specific decoded tables.

{% enddocs %}

{% docs flare_logs_doc %}

The `flare.logs` table contains event logs emitted by smart contracts on the Flare blockchain. It includes:

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
- Tracking FTSO price submissions and rewards
- Monitoring State Connector attestations

{% enddocs %}

{% docs flare_logs_decoded_doc %}

The `flare.logs_decoded` table contains a subset of decoded logs from the Flare blockchain dependent on submitted smart contracts and their ABIs. It includes:

- Block and transaction information
- Contract details (name, namespace, address)
- Decoded event names and signatures
- Transaction origin and destination
- Event parameters (when available)

This table is used for high level analysis of smart contract events, particularly useful for monitoring FTSO and State Connector activities. For fully decoded events and parameters, refer to protocol-specific decoded tables.

{% enddocs %}

{% docs flare_blocks_doc %}

The `flare.blocks` table contains information about Flare blocks. It provides essential data about each block in the Flare blockchain, including:

- Block identifiers and timestamps
- Gas metrics and size
- Consensus information (difficulty, nonce)
- State roots and receipts
- Parent block information
- Base fee per gas
- Extra data

This table is used for:
- Block timing analysis
- Network performance monitoring
- Gas price trends
- Chain reorganization studies
- Consensus metrics tracking

{% enddocs %}

{% docs flare_contracts_doc %}

The `flare.contracts` table contains information about verified smart contracts on the Flare blockchain. It includes:

- Contract address
- Contract name and version
- Verification status and timestamp
- Compiler information
- Source code and ABI
- License type
- Implementation details

This table is used for:
- Contract verification status
- Smart contract analysis
- Protocol research
- Development and debugging
- FTSO and State Connector contract tracking

{% enddocs %}
