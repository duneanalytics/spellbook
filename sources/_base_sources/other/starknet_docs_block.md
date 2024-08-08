{% docs starknet_transactions_doc %}
This table contains information about all transactions on the Starknet blockchain, including both user-initiated and system transactions. Transactions are the actions that modify the state of the blockchain, such as transfers of tokens or the execution of smart contracts. Each transaction is uniquely identified and linked to the block in which it was included.
{% enddocs %}

{% docs starknet_blocks_doc %}
This table represents the blocks in the Starknet blockchain. A block is a collection of transactions, and is identified by a unique block identifier. Each block contains a timestamp and a reference to the previous block hash, forming a chain of blocks. The block structure is crucial for the blockchain’s integrity and security, ensuring a verifiable and tamper-evident ledger.
{% enddocs %}

{% docs starknet_events_doc %}
This table captures the events that are emitted by smart contracts on the Starknet blockchain. Events are used to log significant actions and changes in smart contracts, such as token transfers or updates to contract states. Each event is linked to the transaction that triggered it, providing a detailed audit trail of contract interactions.
{% enddocs %}

