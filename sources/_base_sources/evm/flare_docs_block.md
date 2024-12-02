{% docs flare_transactions_doc %}

The `flare.transactions` table contains detailed information about transactions on the flare blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, from_address, to_address, value
- Gas data: gas_price, gas_limit, gas_used
- Status: success or failure
- Input data for contract interactions
- Nonce
- Transaction type

This table is used for analyzing transaction patterns, gas usage, value transfers, and overall network activity on flare.

{% enddocs %}

{% docs flare_traces_doc %}

The `flare.traces` table contains records of execution steps for transactions on the flare blockchain. Each trace represents an atomic operation that modifies the state of the Ethereum Virtual Machine (EVM). Key components include:

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

{% docs flare_logs_doc %}

The `flare.logs` table contains information about event logs emitted by smart contracts on the flare blockchain. It includes:

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

{% docs flare_contracts_doc %}

The `flare.contracts` table tracks decoded contracts on flare, including:

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
