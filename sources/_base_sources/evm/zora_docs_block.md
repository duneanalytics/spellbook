{% docs zora_transactions_doc %}

The `zora.transactions` table contains detailed information about transactions on the zora blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, from_address, to_address, value
- Gas data: gas_price, gas_limit, gas_used
- Status: success or failure
- Input data for contract interactions
- Nonce
- Transaction type

This table is used for analyzing transaction patterns, gas usage, value transfers, and overall network activity on zora.

{% enddocs %}

{% docs zora_traces_doc %}

The `zora.traces` table contains records of execution steps for transactions on the zora blockchain. Each trace represents an atomic operation that modifies the state of the Ethereum Virtual Machine (EVM). Key components include:

- Transaction hash
- Block number and timestamp
- From and to addresses
- Value transferred
- Input data
- Gas used
- Error information (if applicable)

This table is used for analyzing transaction execution paths, internal contract calls, and state changes within the zora network.

{% enddocs %}

{% docs zora_traces_decoded_doc %}

The `zora.traces_decoded` table contains a subset of decoded traces from the zora blockchain dependent on submitted smart contracts and their ABIs. It includes:

- All fields from the `zora.traces` table
- Decoded function names and signature (first four bytes of the calldata)
- the contract name and schema name we have decoded the function call to

This table is used for high level analysis of smart contract interactions. For fully decoded function calls and parameters, refer to decoded tables such as `uniswap_v3_zora.UniswapV3Pool_call_Swap`. 

{% enddocs %}

{% docs zora_logs_doc %}

The `zora.logs` table contains event logs emitted by smart contracts on the zora blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Topic hashes
- Raw data

This table is used for tracking contract events and state changes on the zora network.

{% enddocs %}

{% docs zora_logs_decoded_doc %}

The `zora.logs_decoded` table contains a subset of decoded logs from the zora blockchain dependent on submitted smart contracts and their ABIs. It includes:

- All fields from the `zora.logs` table
- Decoded event names and signature (topic0 of the log)
- the contract name and schema name we have decoded the event to

This table is used for high level analysis of smart contract events. For fully decoded events and parameters, refer to decoded tables such as `erc20_zora.UniswapV3Pool_evt_Swap`.

{% enddocs %}

{% docs zora_blocks_doc %}

The `zora.blocks` table contains information about zora blocks. It provides essential data about each block in the zora blockchain, including timestamps, gas metrics, and block identifiers. This table is fundamental for analyzing blockchain structure, block production rates, and overall network performance.

{% enddocs %}

{% docs zora_contracts_doc %}

The `zora.contracts` table tracks all contracts that have been submitted to Dune for decoding. It includes:

- metadata about the contract, including its name and namespace
- the contract address

{% enddocs %}

{% docs zora_creation_traces_doc %}

The `zora.creation_traces` table contains data about contract creation events on the zora blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Creator's address
- Created contract address
- deployed contract bytecode
- Gas used for deployment

This table is used for analyzing contract deployment patterns and tracking the origin of smart contracts on the zora network. It's essentially a filtered version of the `zora.traces` table for the condition that `type = create`.

{% enddocs %}

{% docs erc20_zora_evt_transfer_doc %}

The `erc20_zora.evt_transfer` table contains Transfer events for ERC20 tokens on the zora blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- From and to addresses
- Amount transferred

This table is used for tracking ERC20 token movements on the zora network.

Please be aware that this table is the raw ERC20 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `tokens.transfers` for a more complete and curated view of token transfers.

{% enddocs %}

{% docs erc20_zora_evt_approval_doc %}

The `erc20_zora.evt_approval` table contains Approval events for ERC20 tokens on the zora blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and spender addresses
- Approved amount

This table is used for analyzing ERC20 token approvals and spending permissions on the zora network.

{% enddocs %}

{% docs erc1155_zora_evt_transfersingle_doc %}

The `erc1155_zora.evt_transfersingle` table contains TransferSingle events for ERC1155 tokens on the zora blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Operator, from, and to addresses
- Token ID
- Amount transferred

This table is used for tracking individual ERC1155 token transfers on the zora network.

Please be aware that this table is the raw ERC1155 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `nft.transfers` for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc1155_zora_evt_transferbatch_doc %}

The `erc1155_zora.evt_transferbatch` table contains TransferBatch events for ERC1155 tokens on the zora blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Operator, from, and to addresses
- Array of token IDs
- Array of amounts transferred

This table is used for tracking batch transfers of multiple ERC1155 tokens on the zora network.

Please be aware that this table is the raw ERC1155 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use nft.transfers for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc1155_zora_evt_ApprovalForAll_doc %}

The `erc1155_zora.evt_ApprovalForAll` table contains ApprovalForAll events for ERC1155 tokens on the zora blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Account and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC1155 token collections on the zora network.

{% enddocs %}

{% docs erc721_zora_evt_transfer_doc %}

The `erc721_zora.evt_transfer` table contains Transfer events for ERC721 tokens on the zora blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- From and to addresses
- Token ID

This table is used for tracking ERC721 token (NFT) transfers on the zora network.

Please be aware that this table is the raw ERC721 event data, and does not include any additional metadata, context or is in any way filtered or curated. Use `nft.transfers` for a more complete and curated view of NFT transfers.

{% enddocs %}

{% docs erc721_zora_evt_Approval_doc %}

The `erc721_zora.evt_Approval` table contains Approval events for ERC721 tokens on the zora blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and approved addresses
- Token ID

This table is used for analyzing approvals for individual ERC721 tokens (NFTs) on the zora network.

{% enddocs %}

{% docs erc721_zora_evt_ApprovalForAll_doc %}

The `erc721_zora.evt_ApprovalForAll` table contains ApprovalForAll events for ERC721 tokens on the zora blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Owner and operator addresses
- Approved status (boolean)

This table is used for analyzing blanket approvals for ERC721 token collections on the zora network.

{% enddocs %}
