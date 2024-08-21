{% docs tron_transactions_doc %}
The `tron.transactions` table contains detailed information about transactions on the Tron blockchain. It includes:

- Block information: number, timestamp, hash
- Transaction details: hash, from_address, to_address, value
- Transaction type (e.g., TRX transfer, smart contract interaction)
- Status: success or failure
- Energy usage and fees
- Contract data for smart contract interactions

This table is used for analyzing transaction patterns, energy consumption, value transfers, and overall network activity on Tron.
{% enddocs %}

{% docs tron_blocks_doc %}
The `tron.blocks` table contains information about Tron blocks. It provides essential data about each block in the Tron blockchain, including:

- Block number and hash
- Timestamp
- Parent block hash
- Number of transactions
- Block size
- Witness address (block producer)

This table is fundamental for analyzing blockchain structure, block production rates, and overall network performance on the Tron network.
{% enddocs %}

{% docs tron_logs_doc %}
The `tron.logs` table contains event logs emitted by smart contracts on the Tron blockchain. It includes:

- Block number and timestamp
- Transaction hash
- Contract address
- Event topics (including the event signature hash)
- Raw data

This table is used for tracking contract events and state changes on the Tron network, enabling detailed analysis of smart contract interactions and decentralized application (DApp) activity.
{% enddocs %}

