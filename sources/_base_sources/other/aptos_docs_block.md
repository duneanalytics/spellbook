{% docs aptos_blocks_doc %}
This table represents the blocks in the Aptos blockchain. A block is a collection of transactions, and is identified by a unique block identifier. Each block contains a timestamp and a reference to the previous block hash, forming a chain of blocks. The block structure is crucial for the blockchain’s integrity and security, ensuring a verifiable and tamper-evident ledger.
{% enddocs %}

{% docs aptos_events_doc %}
This table captures the events that are emitted by smart contracts on the Aptos blockchain. Events are used to log significant actions and changes in smart contracts, such as token transfers or updates to contract states. Each event is linked to the transaction that triggered it, providing a detailed audit trail of contract interactions.
{% enddocs %}

{% docs aptos_move_modules_doc %}
This table contains data on Move modules deployed on the Aptos blockchain. Move modules contain the bytecode for smart contracts and define the functionality and types used by Move resources. This table provides insight into the smart contract code running on the network, including the module’s author, name, and bytecode.
{% enddocs %}

{% docs aptos_move_resources_doc %}
This table stores information about the Move resources on the Aptos blockchain. Move resources are persistent data structures that are owned by user accounts and can represent various on-chain assets or states. This table includes details on the resource type, the account that owns it, and the resource’s data.
{% enddocs %}

{% docs aptos_move_table_items_doc %}
This table tracks the items stored in Move’s table data structures on the Aptos blockchain. The Move table is a high-performance, typed data structure used for storing and querying on-chain data. This table details the key-value pairs stored, facilitating efficient data access and manipulation within smart contracts.
{% enddocs %}

{% docs aptos_signatures_doc %}
This table documents the signatures associated with transactions on the Aptos blockchain. Signatures are critical for securing transactions, proving the identity of the sender, and ensuring data integrity. This table includes the signature type, data, and the transaction it authenticates, offering insights into the security mechanisms at play.

{% enddocs %}

{% docs aptos_transactions_doc %}
This table contains information about all transactions on the Aptos blockchain, including both user-initiated and system transactions. Transactions are the actions that modify the state of the blockchain, such as transfers of tokens or the execution of smart contracts. Each transaction is uniquely identified and linked to the block in which it was included.
{% enddocs %}

{% docs aptos_user_transactions_doc %}
This table specifically tracks transactions initiated by users of the Aptos blockchain. It includes details such as the sender, the type of transaction, and the gas used, providing insights into how users interact with the network and its smart contracts.
{% enddocs %}

