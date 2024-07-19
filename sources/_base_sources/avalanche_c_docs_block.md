{% docs avalanche_c_transactions_doc %}

## Table Description

The `avalanche_c.transactions` table contains detailed information about transactions on the Avalanche C-Chain, including block data, transaction values, and gas-related metrics. Each row represents a single transaction. The Avalanche C-Chain is EVM-compatible and uses AVAX for gas fees.

{% enddocs %}

{% docs avalanche_c_traces_doc %}

## Table Description

The `avalanche_c.traces` table contains information about traces on the Avalanche C-Chain. An Avalanche C-Chain trace is a small atomic action that modifies the internal state of the Ethereum Virtual Machine. The three main trace types are call, create, and selfdestruct (formerly known as suicide).

{% enddocs %}

{% docs avalanche_c_traces_decoded_doc %}

## Table Description

The `avalanche_c.traces_decoded` table contains decoded Avalanche C-Chain traces, including additional information based on submitted smart contracts and their ABIs.

{% enddocs %}

{% docs avalanche_c_logs_doc %}

## Table Description

The `avalanche_c.logs` table contains information about logs on the Avalanche C-Chain. An Avalanche C-Chain log can be used to describe an event within a smart contract, like a token transfer or a change of ownership.

{% enddocs %}

{% docs avalanche_c_logs_decoded_doc %}

## Table Description

The `avalanche_c.logs_decoded` table contains decoded Avalanche C-Chain logs based on submitted smart contracts, providing human-readable event data.

{% enddocs %}

{% docs avalanche_c_blocks_doc %}

## Table Description

The `avalanche_c.blocks` table contains information about Avalanche C-Chain blocks. Each block contains batches of transactions, linked together in a chain by cryptographic hashes. Avalanche uses the Snowman consensus protocol for its C-Chain.

{% enddocs %}

{% docs avalanche_c_contracts_doc %}

## Table Description

The `avalanche_c.contracts` table is a view tracking decoded contracts on the Avalanche C-Chain, including associated metadata such as namespace, name, address, and ABI.

{% enddocs %}

{% docs avalanche_c_creation_traces_doc %}

## Table Description

The `avalanche_c.creation_traces` table contains information about contract creation traces on the Avalanche C-Chain. It includes details about newly created contracts, their creators, and the contract code.

{% enddocs %}

{% docs erc20_avalanche_c_evt_transfer_doc %}

## Table Description

The `erc20_avalanche_c.evt_transfer` table contains individual transfer events for ERC20 tokens on the Avalanche C-Chain. Each row represents a single token transfer.

{% enddocs %}

{% docs erc20_avalanche_c_evt_approval_doc %}

## Table Description

The `erc20_avalanche_c.evt_approval` table contains approval events for ERC20 tokens on the Avalanche C-Chain, allowing an address to spend tokens on behalf of the owner.

{% enddocs %}

{% docs erc1155_avalanche_c_evt_transfersingle_doc %}

## Table Description

The `erc1155_avalanche_c.evt_transfersingle` table contains single transfer events for ERC1155 tokens on the Avalanche C-Chain.

{% enddocs %}

{% docs erc1155_avalanche_c_evt_transferbatch_doc %}

## Table Description

The `erc1155_avalanche_c.evt_transferbatch` table contains batch transfer events for multiple ERC1155 tokens on the Avalanche C-Chain.

{% enddocs %}

{% docs erc1155_avalanche_c_evt_ApprovalForAll_doc %}

## Table Description

The `erc1155_avalanche_c.evt_ApprovalForAll` table contains approval events for all tokens of an ERC1155 contract on the Avalanche C-Chain.

{% enddocs %}

{% docs erc721_avalanche_c_evt_transfer_doc %}

## Table Description

The `erc721_avalanche_c.evt_transfer` table contains transfer events for ERC721 tokens (NFTs) on the Avalanche C-Chain.

{% enddocs %}

{% docs erc721_avalanche_c_evt_Approval_doc %}

## Table Description

The `erc721_avalanche_c.evt_Approval` table contains approval events for individual ERC721 tokens (NFTs) on the Avalanche C-Chain.

{% enddocs %}

{% docs erc721_avalanche_c_evt_ApprovalForAll_doc %}

## Table Description

The `erc721_avalanche_c.evt_ApprovalForAll` table contains approval events for all tokens of an ERC721 contract on the Avalanche C-Chain.

{% enddocs %}
