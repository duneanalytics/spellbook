{% docs base_transactions_doc %}

## Table Description

The `base.transactions` table contains detailed information about transactions on the Base blockchain, including block data, transaction values, and gas-related metrics. Each row represents a single transaction.

{% enddocs %}

{% docs base_traces_doc %}

## Table Description

The `base.traces` table contains information about traces on the Base blockchain. A Base trace is a small atomic action that modifies the internal state of the Ethereum Virtual Machine. The main trace types include call, create, and suicide.

{% enddocs %}

{% docs base_traces_decoded_doc %}

## Table Description

The `base.traces_decoded` table contains decoded Base traces, including additional information based on submitted smart contracts and their ABIs.

{% enddocs %}

{% docs base_logs_doc %}

## Table Description

The `base.logs` table contains information about event logs emitted by smart contracts on the Base blockchain. A Base log can be used to describe an event within a smart contract, like a token transfer or a change of ownership.

{% enddocs %}

{% docs base_logs_decoded_doc %}

## Table Description

The `base.logs_decoded` table contains decoded Base logs based on submitted smart contracts, providing human-readable event data.

{% enddocs %}

{% docs base_blocks_doc %}

## Table Description

The `base.blocks` table contains information about Base blocks. Each block contains batches of transactions, linked together in a chain by cryptographic hashes.

{% enddocs %}

{% docs base_contracts_doc %}

## Table Description

The `base.contracts` table is a view tracking decoded contracts on Base, including associated metadata such as namespace, name, address, and ABI.

{% enddocs %}

{% docs base_creation_traces_doc %}

## Table Description

The `base.creation_traces` table contains information about contract creation traces on the Base blockchain. It includes details about newly created contracts, their creators, and the contract code.

{% enddocs %}

{% docs erc20_base_evt_transfer_doc %}

## Table Description

The `erc20_base.evt_transfer` table contains individual transfer events for ERC20 tokens on the Base blockchain. Each row represents a single token transfer.

{% enddocs %}

{% docs erc20_base_evt_approval_doc %}

## Table Description

The `erc20_base.evt_approval` table contains approval events for ERC20 tokens on Base, allowing an address to spend tokens on behalf of the owner.

{% enddocs %}

{% docs erc1155_base_evt_transfersingle_doc %}

## Table Description

The `erc1155_base.evt_transfersingle` table contains single transfer events for ERC1155 tokens on the Base blockchain.

{% enddocs %}

{% docs erc1155_base_evt_transferbatch_doc %}

## Table Description

The `erc1155_base.evt_transferbatch` table contains batch transfer events for multiple ERC1155 tokens on the Base blockchain.

{% enddocs %}

{% docs erc1155_base_evt_ApprovalForAll_doc %}

## Table Description

The `erc1155_base.evt_ApprovalForAll` table contains approval events for all tokens of an ERC1155 contract on the Base blockchain.

{% enddocs %}

{% docs erc721_base_evt_transfer_doc %}

## Table Description

The `erc721_base.evt_transfer` table contains transfer events for ERC721 tokens (NFTs) on the Base blockchain.

{% enddocs %}

{% docs erc721_base_evt_Approval_doc %}

## Table Description

The `erc721_base.evt_Approval` table contains approval events for individual ERC721 tokens (NFTs) on the Base blockchain.

{% enddocs %}

{% docs erc721_base_evt_ApprovalForAll_doc %}

## Table Description

The `erc721_base.evt_ApprovalForAll` table contains approval events for all tokens of an ERC721 contract on the Base blockchain.

{% enddocs %}
