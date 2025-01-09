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

{% docs flare_creation_traces_doc %}

The `flare.creation_traces` table contains detailed information about contract creation events on the Flare blockchain. It includes:

- Block information: time, number, date
- Transaction details: hash, from address
- Contract creation specifics:
  * Created contract address
  * Contract bytecode
  * Creation transaction details
  * Success/failure status

This table is essential for:
- Tracking smart contract deployments
- Analyzing contract creation patterns
- Monitoring new protocol deployments
- Auditing contract creation history
- Understanding contract deployment costs

{% enddocs %}

{% docs erc20_flare_evt_transfer_doc %}

The `flare.erc20_flare.evt_transfer` table contains ERC20 token transfer events on the Flare blockchain. Each row represents a single token transfer and includes:

- Contract address of the token
- Transaction details (hash, block info)
- Transfer participants (from and to addresses)
- Amount transferred
- Event metadata (index, block time)

This table is crucial for:
- Tracking token movements
- Analyzing token holder behavior
- Monitoring token activity
- Computing token metrics
- Identifying significant transfers

{% enddocs %}

{% docs erc20_flare_evt_approval_doc %}

The `flare.erc20_flare.evt_approval` table contains ERC20 token approval events on the Flare blockchain. It records when token holders authorize other addresses to spend tokens on their behalf, including:

- Token contract address
- Transaction information
- Owner address (granting approval)
- Spender address (receiving approval)
- Approved amount
- Event metadata

This table is used for:
- Monitoring token approvals
- Analyzing DEX interactions
- Tracking delegation patterns
- Security monitoring
- Protocol integration analysis

{% enddocs %}

{% docs erc1155_flare_evt_transfersingle_doc %}

The `flare.erc1155_flare.evt_transfersingle` table contains single transfer events for ERC1155 tokens on the Flare blockchain. Each record includes:

- Contract address
- Transaction details
- Operator address
- From and to addresses
- Token ID
- Amount transferred
- Event metadata

This table is essential for:
- Tracking multi-token transfers
- Gaming asset movements
- NFT marketplace analysis
- Collection statistics
- User activity monitoring

{% enddocs %}

{% docs erc1155_flare_evt_transferbatch_doc %}

The `flare.erc1155_flare.evt_transferbatch` table contains batch transfer events for ERC1155 tokens on the Flare blockchain. It records multiple token transfers in a single transaction:

- Contract address
- Transaction information
- Operator address
- From and to addresses
- Arrays of token IDs and amounts
- Event metadata

This table is used for:
- Analyzing bulk transfers
- Gaming inventory movements
- Marketplace activity
- Collection migrations
- Protocol efficiency analysis

{% enddocs %}

{% docs erc1155_flare_evt_approvalforall_doc %}

The `flare.erc1155_flare.evt_approvalforall` table contains approval events for ERC1155 tokens on the Flare blockchain. It records when owners grant or revoke approval for all their tokens:

- Contract address
- Transaction details
- Owner address
- Operator address
- Approval status
- Event metadata

This table is crucial for:
- Monitoring collection approvals
- Marketplace integrations
- Protocol permissions
- Security analysis
- User behavior studies

{% enddocs %}

{% docs erc721_flare_evt_transfer_doc %}

The `flare.erc721_flare.evt_transfer` table contains transfer events for ERC721 tokens (NFTs) on the Flare blockchain. Each record represents a single NFT transfer:

- Contract address
- Transaction information
- From and to addresses
- Token ID
- Event metadata

This table is essential for:
- NFT ownership tracking
- Collection analysis
- Market activity monitoring
- User portfolio tracking
- Transfer pattern analysis

{% enddocs %}

{% docs erc721_flare_evt_approval_doc %}

The `flare.erc721_flare.evt_approval` table contains approval events for ERC721 tokens on the Flare blockchain. It records when NFT owners authorize specific addresses to transfer individual tokens:

- Contract address
- Transaction details
- Owner address
- Approved address
- Token ID
- Event metadata

This table is used for:
- NFT approval tracking
- Marketplace integration analysis
- Permission monitoring
- Security auditing
- Protocol interaction study

{% enddocs %}

{% docs erc721_flare_evt_approvalforall_doc %}

The `flare.erc721_flare.evt_approvalforall` table contains collection-wide approval events for ERC721 tokens on the Flare blockchain. It records when owners grant or revoke approval for all their NFTs:

- Contract address
- Transaction information
- Owner address
- Operator address
- Approval status
- Event metadata

This table is crucial for:
- Collection permission tracking
- Marketplace authorization
- Protocol integration analysis
- Security monitoring
- User behavior analysis

{% enddocs %}
