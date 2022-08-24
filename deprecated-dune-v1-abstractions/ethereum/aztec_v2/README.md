# Aztec Connect

These are a series of views and functions to parse and interpret Aztec Connect data. They're broadly separated into a few categories:
* Helper labels
  * contract_labels - Identifies and describes Aztec Connect ecosystem contracts, i.e. rollup processors and bridge contracts. This needs to be updated manually as new bridge contracts are deployed.
  * view_deposit_assets - List of asset by asset ID that the Aztec Rollup Processor interacts with
* Parsing and processing Aztec Connect rollup data
  * fn_process_block - byte data parsing function and struct definitions
  * insert_parsed_rollups - insertion logic and cron job for the rollup parsing function
  * rollups_parsed - table definition for the parsed rollups. More specific data is wrapped up into arrays of structs that are unwrapped in subsequent views.
  * view_rollup_defi_deposits - Unwrapped struct array that describes deposits made into defi protocols from the RollupProcessor
  * view_rollup_inner_proofs - Unwrapped struct array that describes the more specific inner proof data
  * view_rollup_txn_fees - Unwrapped struct array that describes transaction fees collected by the RollupProcessor
* Categorizing and measuring ERC-20 token flows between the Aztec Connect Rollup Processor, bridge contracts, and defi protocols
  * daily_token_prices - table definition for cached daily token price feed
  * insert_daily_token_prices - collates price feed data for any erc20 tokens that are used in the Aztec Connect ecosystem. Preferentially uses `prices.usd`, then prices from dex data with some minimum DQ standards applied
  * view_daily_bridge_activity - Connects each ERC-20 token transfer in the Aztec Connect ecosystem and connects it to the price of those tokens
  * view_daily_deposits - Assumes all non-bridge ERC-20 transfers into the rollup contract are deposits
  * view_daily_estimated_rollup_tvl - Estimates the Rollup Processor's TVL by the value of the balance of ERC20 tokens it holds. This is not strictly accurate, as the rollup can hold positions via the bridge contracts
  * view_rollup_bridge_transfers - Categorizes all ERC-20 token transfers in the Aztec Connect ecosystem

Conceptually, the Aztec Connect bridge boils down defi interactions into transfers of erc-20 token transfers. In general, the flow of tokens is as follows:

**Users** --user deposit--> **Rollup Processor** --defi deposit--> **Bridge Contracts** --defi interactions--> **Defi Protocol**

And then, in reverse:

**Defi Protocol** --defi interactions--> **Bridge Contracts** --defi withdrawal--> **Rollup Processor** --user withdrawal--> **Users**

To track these flows, we can either look at the overtly visible ERC-20 tokens that are being transferred on L1, or we can look at the proof data that Aztec Connect's rollup provides. All defi interactions are obfuscated, but obviously, any user deposits or withdrawals leave L1 traces and are not anonymous.

