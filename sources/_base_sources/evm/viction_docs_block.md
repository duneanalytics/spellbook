{% docs viction_blocks_doc %}
This table contains block-level data from the Viction blockchain. Each row represents a single block and includes details such as the timestamp, block number, hash, gas metrics, and other block-specific information.

Key use cases:
- Analyzing block production and network activity over time
- Monitoring gas usage and block size trends
- Tracking miner/validator behavior

{% enddocs %}

{% docs viction_transactions_doc %}
This table contains transaction-level data from the Viction blockchain. Each row represents a single transaction and includes details such as the sender, receiver, value transferred, gas usage, and transaction status.

Key use cases:
- Analyzing transaction patterns and network usage
- Monitoring gas prices and fees
- Tracking specific address activities
- Investigating transaction success/failure rates

{% enddocs %}

{% docs viction_traces_doc %}
This table contains internal transaction traces from the Viction blockchain. Each row represents an internal call made during transaction execution, providing detailed information about contract interactions and value transfers.

Key use cases:
- Analyzing complex contract interactions
- Tracking internal value transfers
- Debugging failed transactions
- Understanding contract execution flows

{% enddocs %}

{% docs viction_logs_doc %}
This table contains event logs emitted by smart contracts on the Viction blockchain. Each row represents a single event log and includes the emitting contract address, indexed topics, and event data.

Key use cases:
- Tracking specific contract events
- Monitoring token transfers and approvals
- Analyzing contract usage patterns
- Building event-based metrics and analytics

{% enddocs %}

{% docs viction_creation_traces_doc %}
This table contains information about contract creation events on the Viction blockchain. Each row represents a contract deployment transaction and includes details about the creator, created contract, and deployment parameters.

Key use cases:
- Tracking new contract deployments
- Analyzing contract creation patterns
- Monitoring smart contract ecosystem growth
- Investigating contract creators and factory patterns

{% enddocs %}

{% docs erc20_viction_evt_transfer_doc %}
This table contains Transfer events from ERC20 token contracts on the Viction blockchain. Each row represents a token transfer and includes the sender, receiver, and amount transferred.

Key use cases:
- Tracking token transfers and holder activity
- Analyzing token circulation and velocity
- Monitoring specific address token flows
- Building token-specific metrics

{% enddocs %}

{% docs erc20_viction_evt_approval_doc %}
This table contains Approval events from ERC20 token contracts on the Viction blockchain. Each row represents an approval for token spending and includes the token owner, approved spender, and approved amount.

Key use cases:
- Tracking token approvals and delegated spending rights
- Monitoring DEX and protocol integrations
- Analyzing user interaction patterns with token contracts
- Identifying potential security risks from large approvals

{% enddocs %}

{% docs erc1155_viction_evt_transfersingle_doc %}
This table contains TransferSingle events from ERC1155 token contracts on the Viction blockchain. Each row represents a single token transfer and includes the operator, sender, receiver, token ID, and amount.

Key use cases:
- Tracking individual token transfers
- Monitoring NFT and multi-token activities
- Analyzing gaming asset movements
- Building token-specific metrics

{% enddocs %}

{% docs erc1155_viction_evt_transferbatch_doc %}
This table contains TransferBatch events from ERC1155 token contracts on the Viction blockchain. Each row represents a batch token transfer and includes the operator, sender, receiver, token IDs, and amounts.

Key use cases:
- Tracking bulk token transfers
- Monitoring large-scale token operations
- Analyzing gaming asset movements
- Building token-specific metrics

{% enddocs %}

{% docs erc1155_viction_evt_approvalforall_doc %}
This table contains ApprovalForAll events from ERC1155 token contracts on the Viction blockchain. Each row represents an operator approval for all tokens of a contract.

Key use cases:
- Tracking operator approvals for token management
- Monitoring marketplace integrations
- Analyzing user interaction patterns
- Identifying potential security risks

{% enddocs %}

{% docs erc721_viction_evt_transfer_doc %}
This table contains Transfer events from ERC721 token contracts on the Viction blockchain. Each row represents an NFT transfer and includes the sender, receiver, and token ID.

Key use cases:
- Tracking NFT ownership changes
- Analyzing NFT trading patterns
- Monitoring specific collection activity
- Building NFT-specific metrics

{% enddocs %}

{% docs erc721_viction_evt_approval_doc %}
This table contains Approval events from ERC721 token contracts on the Viction blockchain. Each row represents an approval for a specific NFT and includes the owner, approved address, and token ID.

Key use cases:
- Tracking NFT approvals and marketplace listings
- Monitoring NFT trading patterns
- Analyzing marketplace integration activity
- Identifying potential security risks

{% enddocs %}

{% docs erc721_viction_evt_approvalforall_doc %}
This table contains ApprovalForAll events from ERC721 token contracts on the Viction blockchain. Each row represents an operator approval for all NFTs of a contract.

Key use cases:
- Tracking operator approvals for NFT management
- Monitoring marketplace integrations
- Analyzing user interaction patterns
- Identifying potential security risks

{% enddocs %}
