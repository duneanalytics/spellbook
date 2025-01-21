{% docs ink_transactions_doc %}

The `ink.transactions` table contains detailed information about transactions on the Ink blockchain. It includes:

- Block information: block_time, block_number, block_hash, block_date
- Transaction details: hash, from, to, value
- Gas metrics: gas_price, gas_limit, gas_used
- EIP-1559 fee parameters: max_fee_per_gas, max_priority_fee_per_gas, priority_fee_per_gas
- Transaction metadata: nonce, index, success
- Smart contract interaction: data
- Transaction type and access list
- Chain identification: chain_id

This table is used for analyzing transaction patterns, gas usage, value transfers, and network activity on Ink.

{% enddocs %}

{% docs ink_traces_doc %}

The `ink.traces` table contains records of execution steps for transactions on the Ink blockchain. Each trace represents an atomic operation that modifies the blockchain state. Key components include:

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

{% enddocs %}

{% docs ink_traces_decoded_doc %}

The `ink.traces_decoded` table contains decoded traces from verified smart contracts on the Ink blockchain. It includes:

- Block information: block_date, block_time, block_number
- Contract context: namespace, contract_name
- Transaction details: tx_hash, tx_from, tx_to
- Execution path: trace_address
- Function identification: signature, function_name

This table is used for analyzing smart contract interactions with decoded function calls.

{% enddocs %}

{% docs ink_logs_doc %}

The `ink.logs` table contains event logs emitted by smart contracts on the Ink blockchain. It includes:

- Block information: block_time, block_number, block_hash, block_date
- Transaction details: tx_hash, tx_index, tx_from, tx_to
- Contract address
- Event topics: topic0 (event signature), topic1, topic2, topic3
- Event data
- Log position: index

This table is crucial for:
- Tracking on-chain events
- Monitoring contract activity
- Analyzing token transfers
- Following protocol-specific events

{% enddocs %}

{% docs ink_logs_decoded_doc %}

The `ink.logs_decoded` table contains decoded logs from verified smart contracts on the Ink blockchain. It includes:

- Block information: block_date, block_time, block_number
- Contract details: namespace, contract_name, contract_address
- Transaction context: tx_hash, tx_from, tx_to
- Event identification: signature, event_name
- Log position: index

This table is used for analyzing smart contract events with decoded event data.

{% enddocs %}

{% docs ink_blocks_doc %}

The `ink.blocks` table contains information about blocks on the Ink blockchain. It includes:

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

{% enddocs %}

{% docs ink_contracts_doc %}

The `ink.contracts` table tracks verified smart contracts on the Ink blockchain, including:

- Contract address
- Contract bytecode
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

{% docs ink_contracts_submitted_doc %}

The `ink.contracts_submitted` table tracks contracts submitted for verification on the Ink blockchain. It includes:

- Contract address
- Submission metadata (timestamp, submitter)
- Contract name and namespace
- Verification status

This table helps track the progress of contract verification and community contributions.

{% enddocs %}

{% docs ink_creation_traces_doc %}

The `ink.creation_traces` table contains data about contract creation events on the Ink blockchain. It includes:

- Block information: block_time, block_number, block_month
- Transaction details: tx_hash
- Contract details: address, from, code

This table is used for:
- Analyzing contract deployment patterns
- Tracking smart contract origins
- Monitoring protocol deployments
- Understanding contract creation

{% enddocs %}

{% docs erc20_ink_evt_transfer_doc %}

The `erc20_ink.evt_transfer` table contains Transfer events from ERC20 token contracts on the Ink blockchain. Each record represents a token transfer and includes:

- Token contract address
- Sender and recipient addresses
- Amount of tokens transferred
- Block and transaction information
- Event log details

This table is essential for:
- Tracking token transfers
- Analyzing token distribution patterns
- Monitoring token holder behavior
- Calculating token balances
- Understanding token velocity

{% enddocs %}

{% docs erc20_ink_evt_approval_doc %}

The `erc20_ink.evt_approval` table contains Approval events for ERC20 tokens on the ink blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and spender addresses
- Approved amount

This table is used for analyzing ERC20 token approvals and spending permissions on the ink network.

{% enddocs %}

{% docs erc721_ink_evt_transfer_doc %}

The `erc721_ink.evt_transfer` table contains Transfer events from ERC721 (NFT) token contracts on the Ink blockchain. Each record represents an NFT transfer and includes:

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

{% docs erc1155_ink_evt_transfersingle_doc %}

The `erc1155_ink.evt_transfersingle` table contains TransferSingle events for ERC1155 tokens on the ink blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Operator, from, and to addresses
- Token ID
- Amount transferred

This table is used for tracking individual ERC1155 token transfers on the ink network.

Please be aware that this table is the raw ERC1155 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `nft.transfers` for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc1155_ink_evt_transferbatch_doc %}

The `erc1155_ink.evt_transferbatch` table contains TransferBatch events for ERC1155 tokens on the ink blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Operator, from, and to addresses
- Array of token IDs
- Array of amounts transferred

This table is used for tracking batch transfers of multiple ERC1155 tokens on the ink network.

Please be aware that this table is the raw ERC1155 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use nft.transfers for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc1155_ink_evt_ApprovalForAll_doc %}

The `erc1155_ink.evt_ApprovalForAll` table contains ApprovalForAll events for ERC1155 tokens on the ink blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Account and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC1155 token collections on the ink network.

{% enddocs %}

{% docs erc721_ink_evt_Approval_doc %}

The `erc721_ink.evt_Approval` table contains Approval events for ERC721 tokens on the ink blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and approved addresses
- Token ID

This table is used for analyzing approvals for individual ERC721 tokens (NFTs) on the ink network.

{% enddocs %}

{% docs erc721_ink_evt_ApprovalForAll_doc %}

The `erc721_ink.evt_ApprovalForAll` table contains ApprovalForAll events for ERC721 tokens on the ink blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC721 token collections on the ink network.

{% enddocs %}