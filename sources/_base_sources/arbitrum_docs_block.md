{% docs arbitrum_transactions_doc %}

## Table Description

The `arbitrum.transactions` table contains detailed information about transactions on the Arbitrum blockchain, including block data, transaction values, and gas-related metrics. Each row represents a single transaction.

{% enddocs %}

{% docs arbitrum_traces_doc %}

## Table Description

The `arbitrum.traces` table contains information about traces on the Arbitrum blockchain. An Arbitrum trace is a small atomic action that modifies the internal state of the Ethereum Virtual Machine. The three main trace types are call, create, and suicide.

{% enddocs %}

{% docs arbitrum_traces_decoded_doc %}

## Table Description

The `arbitrum.traces_decoded` table contains decoded Arbitrum traces, including additional information based on submitted smart contracts and their ABIs.

{% enddocs %}

{% docs arbitrum_logs_doc %}

## Table Description

The `arbitrum.logs` table contains information about logs on the Arbitrum blockchain. An Arbitrum log can be used to describe an event within a smart contract, like a token transfer or a change of ownership.

{% enddocs %}

{% docs arbitrum_logs_decoded_doc %}

## Table Description

The `arbitrum.logs_decoded` table contains decoded Arbitrum logs based on submitted smart contracts, providing human-readable event data.

{% enddocs %}

{% docs arbitrum_blocks_doc %}

## Table Description

The `arbitrum.blocks` table contains information about Arbitrum blocks. Each block contains batches of transactions, linked together in a chain by cryptographic hashes.

{% enddocs %}

{% docs arbitrum_contracts_doc %}

## Table Description

The `arbitrum.contracts` table is a view tracking decoded contracts on Arbitrum, including associated metadata such as namespace, name, address, and ABI.

{% enddocs %}

{% docs arbitrum_creation_traces_doc %}

## Table Description

The `arbitrum.creation_traces` table contains information about contract creation traces on the Arbitrum blockchain. It includes details about newly created contracts, their creators, and the contract code.

{% enddocs %}

{% docs erc20_arbitrum_evt_transfer_doc %}

## Table Description

The `erc20_arbitrum.evt_transfer` table contains individual transfer events for ERC20 tokens on the Arbitrum blockchain. Each row represents a single token transfer.

{% enddocs %}

{% docs erc20_arbitrum_evt_approval_doc %}

## Table Description

The `erc20_arbitrum.evt_approval` table contains approval events for ERC20 tokens on Arbitrum, allowing an address to spend tokens on behalf of the owner.

{% enddocs %}

{% docs erc1155_arbitrum_evt_transfersingle_doc %}

## Table Description

The `erc1155_arbitrum.evt_transfersingle` table contains single transfer events for ERC1155 tokens on the Arbitrum blockchain.

{% enddocs %}

{% docs erc1155_arbitrum_evt_transferbatch_doc %}

## Table Description

The `erc1155_arbitrum.evt_transferbatch` table contains batch transfer events for multiple ERC1155 tokens on the Arbitrum blockchain.

{% enddocs %}

{% docs erc1155_arbitrum_evt_ApprovalForAll_doc %}

## Table Description

The `erc1155_arbitrum.evt_ApprovalForAll` table contains approval events for all tokens of an ERC1155 contract on the Arbitrum blockchain.

{% enddocs %}

{% docs erc721_arbitrum_evt_transfer_doc %}

## Table Description

The `erc721_arbitrum.evt_transfer` table contains transfer events for ERC721 tokens (NFTs) on the Arbitrum blockchain.

{% enddocs %}

{% docs erc721_arbitrum_evt_Approval_doc %}

## Table Description

The `erc721_arbitrum.evt_Approval` table contains approval events for individual ERC721 tokens (NFTs) on the Arbitrum blockchain.

{% enddocs %}

{% docs erc721_arbitrum_evt_ApprovalForAll_doc %}

## Table Description

The `erc721_arbitrum.evt_ApprovalForAll` table contains approval events for all tokens of an ERC721 contract on the Arbitrum blockchain.

{% enddocs %}
