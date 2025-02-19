{% docs apechain_blocks_doc %}
This table contains information about blocks on the Apechain network.

Each row represents a unique block and includes details such as:
- Block header information (timestamp, number, hash)
- Gas metrics (limit, used, base fee)
- Block size and difficulty
- Block roots (state, transactions, receipts)
- Validator/miner information
{% enddocs %}

{% docs apechain_transactions_doc %}
This table contains information about transactions on the Apechain network.

Each row represents a unique transaction and includes details such as:
- Transaction metadata (block time, block number)
- Value transferred
- Gas information (limit, price, used)
- Transaction participants (from, to addresses)
- Transaction status and type
- Layer 1 related information for rollups
{% enddocs %}

{% docs apechain_logs_doc %}
This table contains event logs emitted by smart contracts on the Apechain network.

Each row represents a unique event log and includes:
- Event metadata (block time, block number)
- Contract information (address)
- Event topics and data
- Transaction information (hash, index)
{% enddocs %}

{% docs apechain_traces_doc %}
This table contains detailed execution traces of transactions on the Apechain network.

Each row represents a single trace (internal transaction) and includes:
- Trace metadata (block time, block number)
- Gas information (allocated, used)
- Transaction context (parent tx info)
- Call information (type, input, output)
- Success/error status
{% enddocs %}

{% docs apechain_creation_traces_doc %}
This table contains information about contract creation traces on the Apechain network.

Each row represents a contract creation event and includes:
- Creation metadata (block time, block number)
- Contract information (address, bytecode)
- Creator address
- Transaction hash
{% enddocs %}

{% docs apechain_contracts_doc %}
This table contains information about verified smart contracts on the Apechain network.

Each row represents a verified smart contract and includes:
- Contract metadata (address, name, namespace)
- Contract code and ABI
- Verification status (dynamic, base, factory)
- Deployment information (creator, timestamp)
{% enddocs %}

{% docs apechain_contracts_submitted_doc %}
This table contains information about manually submitted contract verifications on the Apechain network.

Each row represents a manually submitted contract verification and includes:
- Contract metadata (address, name, namespace)
- Contract code and ABI
- Verification type (dynamic, factory)
- Deployment information (creator, timestamp)
{% enddocs %}

{% docs apechain_traces_decoded_doc %}
This table contains decoded internal transaction (trace) data on the Apechain network.

Each row represents a decoded trace and includes:
- Basic trace information (transaction hash, trace address)
- Decoded input/output data based on contract ABI
- Function signatures and parameter values
- Error information if applicable
{% enddocs %}

{% docs apechain_logs_decoded_doc %}
This table contains decoded event log data from smart contracts on the Apechain network.

Each row represents a decoded event log and includes:
- Basic log information (transaction hash, log index)
- Decoded event data based on contract ABI
- Event name and parameter values
- Contract address and block details
{% enddocs %}

{% docs apechain_erc20_evt_transfer_doc %}
This table contains individual transfer events for ERC20 tokens on the Apechain blockchain. Each row represents a single token transfer event.

Each row includes:
- Basic event information (block number, transaction hash)
- Token contract address
- From and to addresses
- Amount transferred
- Event index and other metadata
{% enddocs %}

{% docs apechain_erc721_evt_transfer_doc %}
This table contains transfer events for ERC721 tokens (NFTs) on the Apechain blockchain.

Each row represents an NFT transfer and includes:
- Basic event information (block number, transaction hash)
- Token contract address
- From and to addresses
- Token ID
- Event index and other metadata
{% enddocs %}

{% docs apechain_erc1155_evt_transfersingle_doc %}
This table contains single transfer events for ERC1155 tokens on the Apechain network.

Each row represents a single token transfer and includes:
- Basic event information (block number, transaction hash)
- Token contract address
- Operator, from, and to addresses
- Token ID and amount
- Event index and other metadata
{% enddocs %}

{% docs apechain_erc1155_evt_transferbatch_doc %}
This table contains batch transfer events for ERC1155 tokens on the Apechain network.

Each row represents a batch token transfer and includes:
- Basic event information (block number, transaction hash)
- Token contract address
- Operator, from, and to addresses
- Array of token IDs and amounts
- Event index and other metadata
{% enddocs %}

{% docs erc20_evt_transfer_doc %}
This table contains ERC20 token transfer events on the Apechain network.

Each row represents a token transfer and includes:
- Basic event information (block number, transaction hash)
- Token contract address
- From and to addresses
- Transfer amount
- Event index and other metadata
{% enddocs %}

{% docs erc721_evt_transfer_doc %}
This table contains ERC721 token transfer events on the Apechain network.

Each row represents an NFT transfer and includes:
- Basic event information (block number, transaction hash)
- Token contract address
- From and to addresses
- Token ID
- Event index and other metadata
{% enddocs %}

{% docs erc1155_evt_transfersingle_doc %}
This table contains single transfer events for ERC1155 tokens on the Apechain network.

Each row represents a token transfer and includes:
- Basic event information (block number, transaction hash)
- Token contract address
- Operator, from, and to addresses
- Token ID and amount
- Event index and other metadata
{% enddocs %}

{% docs erc1155_evt_transferbatch_doc %}
This table contains batch transfer events for ERC1155 tokens on the Apechain network.

Each row represents a batch token transfer and includes:
- Basic event information (block number, transaction hash)
- Token contract address
- Operator, from, and to addresses
- Array of token IDs and amounts
- Event index and other metadata
{% enddocs %}