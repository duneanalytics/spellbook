{% docs base_transactions_doc %}

The `base.transactions` table contains detailed information about transactions on the Base blockchain. It captures essential data such as block details, transaction values, gas metrics, and transaction status. This table is crucial for analyzing transaction patterns, gas usage, and overall network activity on Base.

{% enddocs %}

{% docs base_traces_doc %}

The `base.traces` table provides information about traces on the Base blockchain. Traces represent atomic actions that modify the internal state of the Ethereum Virtual Machine. This table is essential for understanding the execution flow of transactions, including internal contract calls and state changes.

{% enddocs %}

{% docs base_traces_decoded_doc %}

The `base.traces_decoded` table contains decoded Base traces with additional information based on submitted smart contracts and their ABIs. This table enhances the raw trace data by providing human-readable function names and parameter values, making it invaluable for in-depth smart contract interaction analysis.

{% enddocs %}

{% docs base_logs_doc %}

The `base.logs` table contains information about event logs emitted by smart contracts on the Base blockchain. These logs are crucial for tracking specific events within smart contracts, such as token transfers or state changes, providing a detailed view of contract activity and interactions.

{% enddocs %}

{% docs base_logs_decoded_doc %}

The `base.logs_decoded` table contains decoded Base logs based on submitted smart contracts. It transforms raw log data into human-readable event information, making it easier to analyze and understand specific contract events and their parameters.

{% enddocs %}

{% docs base_blocks_doc %}

The `base.blocks` table contains information about Base blocks. It provides essential data about each block in the Base blockchain, including timestamps, gas metrics, and block identifiers. This table is fundamental for analyzing blockchain structure, block production rates, and overall network performance.

{% enddocs %}

{% docs base_contracts_doc %}

The `base.contracts` table tracks decoded contracts on Base, including associated metadata. It serves as a central repository for contract information, enabling easier analysis and interaction with known contracts on the Base network.

{% enddocs %}

{% docs base_creation_traces_doc %}

The `base.creation_traces` table contains information about contract creation traces on the Base blockchain. It provides detailed insights into the deployment of new contracts, including the creator's address and the initial contract code, which is crucial for analyzing smart contract deployment patterns.

{% enddocs %}

{% docs erc20_base_evt_transfer_doc %}

The `erc20_base.evt_transfer` table contains individual transfer events for ERC20 tokens on the Base blockchain. It captures essential data for tracking token movements, enabling analysis of token circulation, user activity, and overall token economy on Base.

{% enddocs %}

{% docs erc20_base_evt_approval_doc %}

The `erc20_base.evt_approval` table contains approval events for ERC20 tokens on Base. It records instances where token owners grant spending permissions to other addresses, which is crucial for understanding token delegation patterns and integrated DeFi activities.

{% enddocs %}

{% docs erc1155_base_evt_transfersingle_doc %}

The `erc1155_base.evt_transfersingle` table contains single transfer events for ERC1155 tokens on the Base blockchain. It tracks individual transfers of these multi-token standard assets, providing insights into the movement of both fungible and non-fungible tokens within a single contract.

{% enddocs %}

{% docs erc1155_base_evt_transferbatch_doc %}

The `erc1155_base.evt_transferbatch` table contains batch transfer events for multiple ERC1155 tokens on the Base blockchain. It captures bulk transfers of tokens, allowing for efficient analysis of large-scale token movements and complex token interactions.

{% enddocs %}

{% docs erc1155_base_evt_ApprovalForAll_doc %}

The `erc1155_base.evt_ApprovalForAll` table contains approval events for all tokens of an ERC1155 contract on the Base blockchain. It records blanket approvals for operators to manage all tokens of an owner, which is essential for understanding delegation patterns in ERC1155 token ecosystems.

{% enddocs %}

{% docs erc721_base_evt_transfer_doc %}

The `erc721_base.evt_transfer` table contains transfer events for ERC721 tokens (NFTs) on the Base blockchain. It tracks the movement of unique tokens, providing crucial data for analyzing NFT ownership changes, market activity, and collection popularity.

{% enddocs %}

{% docs erc721_base_evt_Approval_doc %}

The `erc721_base.evt_Approval` table contains approval events for individual ERC721 tokens (NFTs) on the Base blockchain. It records permissions granted for specific NFTs, which is vital for understanding NFT marketplace interactions and delegation patterns.

{% enddocs %}

{% docs erc721_base_evt_ApprovalForAll_doc %}

The `erc721_base.evt_ApprovalForAll` table contains approval events for all tokens of an ERC721 contract on the Base blockchain. It captures blanket approvals for operators to manage all NFTs of an owner within a collection, essential for analyzing NFT management strategies and marketplace integrations.

{% enddocs %}
