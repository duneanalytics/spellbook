{% docs shape_blocks_doc %}

The `shape.blocks` table contains information about blocks on the Shape blockchain. It includes:

- Block identifiers: number, hash, time, date
- Gas metrics: gas_limit, gas_used, blob_gas_used, excess_blob_gas
- Block characteristics: size, base_fee_per_gas
- Block roots: state_root, transactions_root, receipts_root, parent_beacon_block_root
- Consensus data: difficulty, total_difficulty, nonce
- Block producer: miner
- Parent block: parent_hash

This table is fundamental for analyzing:
- Block production and timing
- Network capacity and usage
- Chain structure and growth
- Network performance metrics
- Blob gas usage patterns

As Shape is a culture-first Ethereum L2 focused on NFTs, this table provides essential data for understanding the underlying blockchain infrastructure that powers the NFT ecosystem on Shape.

{% enddocs %}

{% docs shape_transactions_doc %}

The `shape.transactions` table contains detailed information about transactions on the Shape blockchain. It includes:

- Block information: block_time, block_number, block_hash, block_date
- Transaction details: hash, from, to, value
- Gas metrics: gas_price, gas_limit, gas_used
- EIP-1559 fee parameters: max_fee_per_gas, max_priority_fee_per_gas, priority_fee_per_gas
- Transaction metadata: nonce, index, success
- Smart contract interaction: data
- Transaction type and access list
- Chain identification: chain_id
- L1 related data: l1_gas_used, l1_gas_price, l1_fee, l1_fee_scalar, l1_block_number, l1_timestamp, l1_tx_origin

This table is used for analyzing:
- Transaction patterns and volume
- Gas usage and fee trends
- Smart contract interactions
- Network activity and usage
- L1/L2 interactions and costs

Shape's Gasback feature allows contract owners to claim back 80% of the sequencer fees generated from onchain activity, making transaction analysis particularly valuable for understanding creator rewards and network economics.

{% enddocs %}

{% docs shape_traces_doc %}

The `shape.traces` table contains records of execution steps for transactions on the Shape blockchain. Each trace represents an atomic operation that modifies the blockchain state. Key components include:

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
- Monitoring NFT-related operations at a granular level

As Shape focuses on creating a web of NFTs that can seamlessly interact with each other, this traces table provides deep insights into the programmatic interactions between digital objects on the network.

{% enddocs %}

{% docs shape_traces_decoded_doc %}

The `shape.traces_decoded` table contains decoded traces with additional information based on submitted smart contracts and their ABIs. It includes:

- Block information: block_date, block_time, block_number
- Contract details: namespace, contract_name
- Transaction context: tx_hash, tx_from, tx_to
- Function details: signature, function_name
- Trace location: trace_address

This table is used for:
- Analyzing smart contract interactions
- Monitoring protocol operations
- Debugging contract calls
- Understanding function call patterns
- Tracking internal transactions

For Shape's NFT ecosystem, this table provides valuable insights into how NFT contracts interact with each other, helping developers and analysts understand the web of NFTs that form the backbone of Shape's digital economy.

{% enddocs %}

{% docs shape_creation_traces_doc %}

The `shape.creation_traces` table contains data about contract creation events on the Shape blockchain. It includes:

- Block information: block_time, block_number, block_month
- Transaction details: tx_hash
- Contract details: address, from, code

This table is used for:
- Analyzing contract deployment patterns
- Tracking smart contract origins
- Monitoring NFT contract deployments
- Understanding contract creation on Shape's NFT-focused ecosystem

Shape's emphasis on NFTs and creator economy makes this table particularly valuable for tracking the deployment of NFT contracts and other creative applications on the network.

{% enddocs %}

{% docs shape_logs_doc %}

The `shape.logs` table contains event logs emitted by smart contracts on the Shape blockchain. It includes:

- Block information: block_time, block_number, block_hash, block_date
- Transaction details: tx_hash, tx_index, tx_from, tx_to
- Contract address
- Event topics: topic0 (event signature), topic1, topic2, topic3
- Event data
- Log position: index

This table is crucial for:
- Tracking on-chain events
- Monitoring NFT contract activity
- Analyzing token transfers
- Following protocol-specific events
- Understanding smart contract interactions

As Shape is designed to be "The NFT chain" where digital objects can seamlessly interact with each other, this logs table provides essential data for analyzing the web of NFT interactions and creator activity on the network.

{% enddocs %}

{% docs shape_logs_decoded_doc %}

The `shape.logs_decoded` table contains decoded event logs with additional information based on submitted smart contracts and their ABIs. It includes:

- Block information: block_date, block_time, block_number
- Contract details: namespace, contract_name, contract_address
- Transaction context: tx_hash, tx_from, tx_to
- Event details: signature, event_name
- Log position: index

This table is used for:
- Analyzing decoded smart contract events
- Monitoring protocol operations
- Tracking NFT transfers with human-readable event names
- Understanding contract interactions
- Protocol-specific event analysis

Shape's focus on creating a web of NFTs that can programmatically interact with each other makes this table particularly valuable for understanding the complex interactions between digital objects on the network.

{% enddocs %}

{% docs erc20_shape_evt_transfer_doc %}

The `erc20_shape.evt_transfer` table contains Transfer events for ERC20 tokens on the Shape blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- From and to addresses
- Amount transferred

This table is used for tracking ERC20 token movements on the Shape network, which may include utility tokens that support the NFT ecosystem.

Please be aware that this table is the raw ERC20 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `tokens.transfers` for a more complete and curated view of token transfers.

{% enddocs %}

{% docs erc20_shape_evt_approval_doc %}

The `erc20_shape.evt_approval` table contains Approval events for ERC20 tokens on the Shape blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and spender addresses
- Approved amount

This table is used for analyzing ERC20 token approvals and spending permissions on the Shape network, which may be relevant for understanding token utility within the NFT ecosystem.

{% enddocs %}

{% docs erc721_shape_evt_transfer_doc %}

The `erc721_shape.evt_transfer` table contains Transfer events for ERC721 tokens (NFTs) on the Shape blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- From and to addresses
- Token ID

This table is especially important on Shape as it is designed to be "The NFT chain" and is used for tracking NFT transfers and ownership changes. Shape's Gasback feature allows contract owners to claim back 80% of the sequencer fees generated from onchain activity, making NFT transfers particularly significant for creators.

Please be aware that this table is the raw ERC721 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `nft.transfers` for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc721_shape_evt_approval_doc %}

The `erc721_shape.evt_approval` table contains Approval events for ERC721 tokens (NFTs) on the Shape blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner, approved addresses
- Token ID

This table is used for analyzing NFT approvals for individual tokens on the Shape network, which is relevant for understanding NFT marketplace interactions and permissions.

{% enddocs %}

{% docs erc721_shape_evt_ApprovalForAll_doc %}

The `erc721_shape.evt_ApprovalForAll` table contains ApprovalForAll events for ERC721 tokens (NFTs) on the Shape blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for NFT collections on the Shape network, which is particularly relevant for marketplace integrations and collection-wide permissions in Shape's creator-focused ecosystem.

{% enddocs %}

{% docs erc1155_shape_evt_transfersingle_doc %}

The `erc1155_shape.evt_transfersingle` table contains TransferSingle events for ERC1155 tokens on the Shape blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Operator, from, and to addresses
- Token ID and value

This table is used for tracking individual ERC1155 token transfers on the Shape network, which may represent semi-fungible assets within the NFT ecosystem.

Please be aware that this table is the raw ERC1155 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `nft.transfers` for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc1155_shape_evt_transferbatch_doc %}

The `erc1155_shape.evt_transferbatch` table contains TransferBatch events for ERC1155 tokens on the Shape blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Operator, from, and to addresses
- Array of token IDs and values

This table is used for tracking batch transfers of multiple ERC1155 tokens on the Shape network, which is relevant for understanding complex NFT transactions and collection movements.

Please be aware that this table is the raw ERC1155 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `nft.transfers` for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc1155_shape_evt_ApprovalForAll_doc %}

The `erc1155_shape.evt_ApprovalForAll` table contains ApprovalForAll events for ERC1155 tokens on the Shape blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Account and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC1155 token collections on the Shape network, which is relevant for marketplace integrations and permissions in Shape's NFT ecosystem.

{% enddocs %}
