{% docs opbnb_transactions_doc %}

The `opbnb.transactions` table contains detailed information about transactions on the opBNB blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, from_address, to_address, value
- Gas data: gas_price, gas_limit, gas_used
- Status: success or failure
- Input data for contract interactions
- Nonce
- Transaction type
- L1 data: l1_gas_used, l1_gas_price, l1_fee, l1_fee_scalar (specific to L2 chains)

This table is used for analyzing transaction patterns, gas usage, value transfers, and overall network activity on opBNB.

{% enddocs %}

{% docs opbnb_traces_doc %}

The `opbnb.traces` table contains records of execution steps for transactions on the opBNB blockchain. Each trace represents an atomic operation that modifies the state of the Ethereum Virtual Machine (EVM). Key components include:

- Transaction hash
- Block number and timestamp
- From and to addresses
- Value transferred
- Input data
- Gas used
- Error information (if applicable)

This table is used for analyzing transaction execution paths, internal contract calls, and state changes within the opBNB network.

{% enddocs %}

{% docs opbnb_logs_doc %}

The `opbnb.logs` table contains event logs emitted by smart contracts on the opBNB blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Event topics (up to 4)
- Event data
- Log index

This table is used for analyzing smart contract events, token transfers, and other on-chain activities on opBNB.

{% enddocs %}

{% docs opbnb_blocks_doc %}

The `opbnb.blocks` table contains information about blocks on the opBNB blockchain. It includes:

- Block number and timestamp
- Block hash and parent hash
- Gas limit and gas used
- Base fee per gas
- Size
- Miner address
- State, transactions, and receipts roots
- Difficulty and total difficulty

This table is used for analyzing block production, gas usage patterns, and network activity on opBNB.

{% enddocs %}

{% docs opbnb_creation_traces_doc %}

The `opbnb.creation_traces` table contains information about contract creation events on the opBNB blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Creator address
- Created contract address
- Contract bytecode

This table is used for analyzing smart contract deployments and tracking the creation of new contracts on opBNB.

{% enddocs %}

{% docs opbnb_erc20_transfers_doc %}

The `opbnb.erc20_transfers` table contains information about ERC-20 token transfers on the opBNB blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Token contract address
- From and to addresses
- Amount transferred

This table is used for analyzing token transfers, token holder activity, and token circulation on opBNB.

{% enddocs %}

{% docs opbnb_erc721_transfers_doc %}

The `opbnb.erc721_transfers` table contains information about ERC-721 (NFT) transfers on the opBNB blockchain. It includes:

- Block number and timestamp
- Transaction hash
- NFT contract address
- From and to addresses
- Token ID

This table is used for analyzing NFT transfers, ownership changes, and NFT market activity on opBNB.

{% enddocs %}

{% docs opbnb_erc1155_transfers_single_doc %}

The `opbnb.erc1155_transfers_single` table contains information about single ERC-1155 token transfers on the opBNB blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Token contract address
- From and to addresses
- Token ID
- Value (amount) transferred

This table is used for analyzing ERC-1155 token transfers and multi-token activity on opBNB.

{% enddocs %}

{% docs opbnb_erc1155_transfers_batch_doc %}

The `opbnb.erc1155_transfers_batch` table contains information about batch ERC-1155 token transfers on the opBNB blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Token contract address
- From and to addresses
- Token IDs (array)
- Values (amounts) transferred (array)

This table is used for analyzing batch transfers of ERC-1155 tokens on opBNB.

{% enddocs %}
