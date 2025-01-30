{% docs abstract_blocks_doc %}

The `abstract.blocks` table contains information about blocks on the Abstract blockchain. It includes:

- Block identifiers: number, hash, time, date
- Gas metrics: gas_limit, gas_used
- Block characteristics: size, base_fee_per_gas
- Block roots: state_root, transactions_root, receipts_root
- Consensus data: difficulty, total_difficulty, nonce
- Block producer: miner
- Parent block: parent_hash

This table is fundamental for analyzing:
- Block production and timing
- Network capacity and usage
- Chain structure and growth
- Network performance metrics

{% enddocs %}

{% docs abstract_transactions_doc %}

The `abstract.transactions` table contains detailed information about transactions on the Abstract blockchain. It includes:

- Block information: block_time, block_number, block_hash, block_date
- Transaction details: hash, from, to, value
- Gas metrics: gas_price, gas_limit, gas_used
- EIP-1559 fee parameters: max_fee_per_gas, max_priority_fee_per_gas, priority_fee_per_gas
- Transaction metadata: nonce, index, success
- Smart contract interaction: data
- Transaction type and access list
- Chain identification: chain_id
- L1 batch information: l1_batch_number, l1_batch_tx_index

This table is used for analyzing:
- Transaction patterns and volume
- Gas usage and fee trends
- Smart contract interactions
- Network activity and usage
- L1/L2 batch processing

{% enddocs %}

{% docs abstract_traces_doc %}

The `abstract.traces` table contains records of execution steps for transactions on the Abstract blockchain. Each trace represents an atomic operation that modifies the blockchain state. Key components include:

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

{% docs abstract_logs_doc %}

The `abstract.logs` table contains event logs emitted by smart contracts on the Abstract blockchain. It includes:

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

{% docs abstract_logs_decoded_doc %}

The `abstract.logs_decoded` table contains decoded logs from verified smart contracts on the Abstract blockchain. It includes:

- Block information: block_date, block_time, block_number
- Contract details: namespace, contract_name, contract_address
- Transaction context: tx_hash, tx_from, tx_to
- Event identification: signature, event_name
- Log position: index

This table is used for analyzing smart contract events with decoded event data, making it easier to:
- Track specific contract events
- Monitor protocol operations
- Analyze token transfers with human-readable event names
- Debug smart contract interactions

{% enddocs %}

{% docs abstract_creation_traces_doc %}

The `abstract.creation_traces` table contains data about contract creation events on the Abstract blockchain. It includes:

- Block information: block_time, block_number, block_month
- Transaction details: tx_hash
- Contract details: address, from, code

This table is used for:
- Analyzing contract deployment patterns
- Tracking smart contract origins
- Monitoring protocol deployments
- Understanding contract creation

{% enddocs %}

{% docs erc20_abstract_evt_transfer_doc %}

The `erc20_abstract.evt_transfer` table contains Transfer events from ERC20 token contracts on the Abstract blockchain. Each record represents a token transfer and includes:

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

{% docs erc20_abstract_evt_approval_doc %}

The `erc20_abstract.evt_approval` table contains Approval events for ERC20 tokens on the abstract blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and spender addresses
- Approved amount

This table is used for analyzing ERC20 token approvals and spending permissions on the abstract network.

{% enddocs %}

{% docs erc721_abstract_evt_transfer_doc %}

The `erc721_abstract.evt_transfer` table contains Transfer events from ERC721 (NFT) token contracts on the Abstract blockchain. Each record represents an NFT transfer and includes:

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

{% docs erc721_abstract_evt_Approval_doc %}

The `erc721_abstract.evt_Approval` table contains Approval events for ERC721 tokens on the abstract blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and approved addresses
- Token ID

This table is used for analyzing approvals for individual ERC721 tokens (NFTs) on the abstract network.

{% enddocs %}

{% docs erc721_abstract_evt_ApprovalForAll_doc %}

The `erc721_abstract.evt_ApprovalForAll` table contains ApprovalForAll events for ERC721 tokens on the abstract blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC721 token collections on the abstract network.

{% enddocs %}

{% docs erc1155_abstract_evt_transfersingle_doc %}

The `erc1155_abstract.evt_transfersingle` table contains TransferSingle events for ERC1155 tokens on the abstract blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Operator, from, and to addresses
- Token ID
- Amount transferred

This table is used for tracking individual ERC1155 token transfers on the abstract network.

Please be aware that this table is the raw ERC1155 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `nft.transfers` for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc1155_abstract_evt_transferbatch_doc %}

The `erc1155_abstract.evt_transferbatch` table contains TransferBatch events for ERC1155 tokens on the abstract blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Operator, from, and to addresses
- Array of token IDs
- Array of amounts transferred

This table is used for tracking batch transfers of multiple ERC1155 tokens on the abstract network.

Please be aware that this table is the raw ERC1155 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use nft.transfers for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc1155_abstract_evt_ApprovalForAll_doc %}

The `erc1155_abstract.evt_ApprovalForAll` table contains ApprovalForAll events for ERC1155 tokens on the abstract blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Account and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC1155 token collections on the abstract network.

{% enddocs %} 