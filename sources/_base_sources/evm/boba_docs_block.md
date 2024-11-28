{% docs boba_transactions_doc %}

The `boba.transactions` table contains detailed information about transactions on the boba blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, from_address, to_address, value
- Gas data: gas_price, gas_limit, gas_used
- Status: success or failure
- Input data for contract interactions
- Nonce
- Transaction type

This table is used for analyzing transaction patterns, gas usage, value transfers, and overall network activity on boba.

{% enddocs %}

{% docs boba_traces_doc %}

The `boba.traces` table contains records of execution steps for transactions on the boba blockchain. Each trace represents an atomic operation that modifies the state of the Ethereum Virtual Machine (EVM). Key components include:

- Transaction hash
- Block number and timestamp
- From and to addresses
- Value transferred
- Input data
- Call type (e.g., CALL, DELEGATECALL, CREATE)
- Gas information
- Error messages (if any)

This table is essential for:
- Analyzing internal transactions
- Debugging smart contract interactions
- Tracking value flows through complex transactions
- Understanding contract creation and deployment

{% enddocs %}

{% docs boba_logs_doc %}

The `boba.logs` table contains information about event logs emitted by smart contracts on the boba blockchain. It includes:

- Block information: number, timestamp
- Transaction hash
- Contract address (emitting the event)
- Topic0 (event signature)
- Additional topics (indexed parameters)
- Data field (non-indexed parameters)
- Log index

This table is crucial for:
- Tracking on-chain events
- Monitoring contract activity
- Analyzing token transfers
- Following protocol-specific events

{% enddocs %}

{% docs boba_contracts_doc %}

The `boba.contracts` table tracks decoded contracts on boba, including:

- Contract address
- Bytecode
- Contract name
- Namespace
- ABI
- Creation details

This table is used for:
- Contract verification
- Smart contract analysis
- Protocol research
- Development and debugging

{% enddocs %}
