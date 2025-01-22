{% docs bitcoin_blocks_doc %}

## Table Description

The `bitcoin.blocks` table contains information about blocks on the Bitcoin blockchain. Each row represents a single block, including details such as the block time, height, hash, transaction count, size, mining rewards, difficulty, and various blockchain-specific information like merkle root and nonce.

{% enddocs %}

{% docs bitcoin_transactions_doc %}

## Table Description

The `bitcoin.transactions` table contains information about transactions on the Bitcoin blockchain. Each row represents a single transaction, including details such as the block it was included in, transaction ID, input and output values, fees, size, and whether it's a coinbase transaction. It also includes information about the transaction's inputs and outputs, as well as its lock time and raw hexadecimal representation.

{% enddocs %}

{% docs bitcoin_inputs_doc %}

## Table Description

The `bitcoin.inputs` table contains information about transaction inputs on the Bitcoin blockchain. Each row represents a single input, including details such as the block and transaction it belongs to, the previous output being spent, input value, associated address, and script information. It also includes data specific to coinbase inputs and witness data for SegWit transactions.

{% enddocs %}

{% docs bitcoin_outputs_doc %}

## Table Description

The `bitcoin.outputs` table contains information about transaction outputs on the Bitcoin blockchain. Each row represents a single output, including details such as the block and transaction it belongs to, output index, value, recipient address, and script information. This table is crucial for tracking the creation of new unspent transaction outputs (UTXOs) in the Bitcoin network.

{% enddocs %}