{% docs solana_transactions_doc %}
The `solana.transactions` table contains detailed information about transactions on the Solana blockchain. It includes data such as account keys, block information, transaction details, and more. Key columns include:

- `account_keys`: Array of addresses involved in the transaction
- `block_date`, `block_hash`, `block_slot`, `block_time`: Information about the block containing the transaction
- `fee`: Transaction fee
- `id`: Unique transaction identifier
- `instructions`: Details of the transaction instructions
- `signatures`: Transaction signatures
- `success`: Indicates whether the transaction was successful
{% enddocs %}

{% docs solana_rewards_doc %}
The `solana.rewards` table provides information about rewards on the Solana blockchain. It captures details about reward distributions, including:

- `block_date`, `block_hash`, `block_slot`, `block_time`: Information about the block where the reward was processed
- `lamports`: Number of reward lamports credited or debited by the account
- `pre_balances` and `post_balances`: Account balances before and after the reward was applied
- `recipient`: Public key of the account that received the reward
- `reward_type`: Type of reward (e.g., 'Fee', 'Rent', 'Voting', 'Staking')
- `commission`: Vote account commission for voting and staking rewards
{% enddocs %}

{% docs solana_account_activity_doc %}
The `solana.account_activity` table tracks detailed account activity on the Solana blockchain. It includes information such as:

- `block_slot`, `block_hash`, `block_time`, `block_date`: Block details for the activity
- `address`: The address (public key) of the account
- `tx_id`, `tx_index`, `tx_success`: Transaction information
- `signed`: Indicates if the account signed the transaction
- `writeable`: Shows if the account had read-write access in the transaction
- `pre_balance`, `post_balance`, `balance_change`: Account balance information
- `pre_token_balance`, `post_token_balance`, `token_balance_change`: Token balance details
- `token_mint_address`: Address of the associated mint token
{% enddocs %}

{% docs solana_instruction_calls_doc %}
The `solana.instruction_calls` table provides detailed information about instruction calls within Solana transactions. Key columns include:

- `block_slot`, `block_hash`, `block_time`, `block_date`: Block information
- `index`, `tx_index`: Order and position of the instruction
- `outer_instruction_index`, `inner_instruction_index`: Indexes for outer and inner instructions
- `executing_account`: Account key of the program that executed the instruction
- `data`: Program input data
- `is_inner`: Indicates if the instruction is an inner instruction
- `account_arguments`: List of accounts passed to the program
- `tx_id`, `tx_success`: Transaction identifier and status
- `log_messages`: Log messages emitted by the transaction
{% enddocs %}

{% docs solana_vote_transactions_doc %}
The `solana.vote_transactions` table contains only vote transactions. The votes are transactions that are not executed but are used to select the leader for a given block. Key columns include:

- `block_slot`, `block_hash`, `block_time`, `block_date`: Block information
- `tx_hash`: The unique identifier of the transaction
- `tx_from`: The address that initiates the transaction and pays the transaction fee
- `tx_fee_raw`: The raw transaction fee in lamports
- `block_proposer`: The leader who proposed the block
- `tx_type`: The type of the transaction
- `limit_type`: The type of compute limit applied to the transaction
{% enddocs %}


