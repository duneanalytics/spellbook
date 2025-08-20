{% docs katana_blocks_doc %}

The `katana.blocks` table contains information about blocks on the katana blockchain. It includes:

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

{% enddocs %}

{% docs katana_transactions_doc %}

The `katana.transactions` table contains detailed information about transactions on the katana blockchain. It includes:

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

{% enddocs %}

{% docs katana_logs_doc %}

The `katana.logs` table contains event logs emitted by smart contracts on the katana blockchain. It includes:

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
- Understanding smart contract interactions

{% enddocs %}

{% docs katana_traces_doc %}

The `katana.traces` table contains records of execution steps for transactions on the katana blockchain. Each trace represents an atomic operation that modifies the blockchain state. Key components include:

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

{% docs katana_creation_traces_doc %}

The `katana.creation_traces` table contains data about contract creation events on the katana blockchain. It includes:

- Block information: block_time, block_number, block_month
- Transaction details: tx_hash
- Contract details: address, from, code

This table is used for:
- Analyzing contract deployment patterns
- Tracking smart contract origins
- Monitoring protocol deployments
- Understanding contract creation

{% enddocs %}

{% docs katana_contracts_doc %}

The `katana.contracts` table contains information about verified smart contracts on the katana blockchain. It includes:

- Contract identification: address, name, namespace
- Contract code and ABI
- Deployment information: from, created_at
- Contract type flags: dynamic, base, factory
- Verification metadata: abi_id, detection_source

This table is essential for:
- Smart contract analysis
- Protocol tracking
- Contract verification status
- Understanding contract relationships
- Contract deployment monitoring

{% enddocs %}

{% docs katana_contracts_submitted_doc %}

The `katana.contracts_submitted` table contains information about manually submitted contract verifications on the katana blockchain. It includes:

- Contract identification: address, name, namespace
- Contract code and ABI
- Deployment information: from, created_at
- Contract type flags: dynamic, factory

This table is used for:
- Tracking manual contract verifications
- Contract deployment analysis
- Contract code verification
- Protocol monitoring

{% enddocs %}

{% docs katana_traces_decoded_doc %}

The `katana.traces_decoded` table contains decoded traces with additional information based on submitted smart contracts and their ABIs. It includes:

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

{% enddocs %}

{% docs katana_logs_decoded_doc %}

The `katana.logs_decoded` table contains decoded event logs with additional information based on submitted smart contracts and their ABIs. It includes:

- Block information: block_date, block_time, block_number
- Contract details: namespace, contract_name, contract_address
- Transaction context: tx_hash, tx_from, tx_to
- Event details: signature, event_name
- Log position: index

This table is used for:
- Analyzing decoded smart contract events
- Monitoring protocol operations
- Tracking token transfers with human-readable event names
- Understanding contract interactions
- Protocol-specific event analysis

{% enddocs %}

{% docs erc20_katana_evt_Transfer_doc %}

The `erc20_katana.evt_transfer` table contains Transfer events for ERC20 tokens on the katana blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- From and to addresses
- Amount transferred

This table is used for tracking ERC20 token movements on the katana network.

Please be aware that this table is the raw ERC20 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `tokens.transfers` for a more complete and curated view of token transfers.

{% enddocs %}

{% docs erc20_katana_evt_Approval_doc %}

The `erc20_katana.evt_Approval` table contains Approval events for ERC20 tokens on the katana blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and spender addresses
- Approved amount

This table is used for analyzing ERC20 token approvals and spending permissions on the katana network.

{% enddocs %}

{% docs erc1155_katana_evt_TransferSingle_doc %}

The `erc1155_katana.evt_TransferSingle` table contains TransferSingle events for ERC1155 tokens on the katana blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Operator, from, and to addresses
- Token ID
- Amount transferred

This table is used for tracking individual ERC1155 token transfers on the katana network.

Please be aware that this table is the raw ERC1155 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `nft.transfers` for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc1155_katana_evt_TransferBatch_doc %}

The `erc1155_katana.evt_TransferBatch` table contains TransferBatch events for ERC1155 tokens on the katana blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Operator, from, and to addresses
- Array of token IDs
- Array of amounts transferred

This table is used for tracking batch transfers of multiple ERC1155 tokens on the katana network.

Please be aware that this table is the raw ERC1155 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use nft.transfers for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc1155_katana_evt_ApprovalForAll_doc %}

The `erc1155_katana.evt_ApprovalForAll` table contains ApprovalForAll events for ERC1155 tokens on the katana blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Account and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC1155 token collections on the katana network.

{% enddocs %}

{% docs erc721_katana_evt_Transfer_doc %}

The `erc721_katana.evt_Transfer` table contains Transfer events for ERC721 tokens on the katana blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- From and to addresses
- Token ID

This table is used for tracking ERC721 token (NFT) transfers on the katana network.

Please be aware that this table is the raw ERC721 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `nft.transfers` for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc721_katana_evt_Approval_doc %}

The `erc721_katana.evt_Approval` table contains Approval events for ERC721 tokens on the katana blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and approved addresses
- Token ID

This table is used for analyzing approvals for individual ERC721 tokens (NFTs) on the katana network.

{% enddocs %}

{% docs erc721_katana_evt_ApprovalForAll_doc %}

The `erc721_katana.evt_ApprovalForAll` table contains ApprovalForAll events for ERC721 tokens on the katana blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC721 token collections on the katana network.
{% enddocs %}

