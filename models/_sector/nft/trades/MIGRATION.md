# Sector: NFT

We're migrating the NFT sector spells to a new structure which will make implementing and maintaining marketplace abstractions a lot easier and more robust.

## `nft.trades`

- partition_by: `[blockchain, block_date]` 
- unique_key: `[blockchain, project, project_version, block_number,tx_hash, sub_tx_trade_id]`

| Column Name             | Descriptions                                                       |
|-------------------------|--------------------------------------------------------------------|
| **blockchain**          | The name of the blockchain on which the NFT trade occurred         |
| **block_number**        | The number of the block in which the NFT trade occurred            |
| block_time              | The timestamp of the block in which the NFT trade occurred         |
| **tx_hash**             | The hash of the transaction in which the NFT trade occurred        |
| **sub_tx_trade_id**     | A unique identifier of the NFT trade within one transaction        |
| **project**             | The name of the project/marketplace executing the trade            |
| **project_version**     | The version of the project/marketplace executing the trade         |
| trade_category          | The category of the NFT trade (buy, sell, swap)                    |
| trade_type              | Primary or Secondary sale                                          |
| buyer                   | The address of the buyer                                           |
| seller                  | The address of the seller                                          |
| nft_collection          | The name of the collection or series to which the NFT belongs      |
| nft_contract_address    | The NFT contract address                                           |
| nft_standard            | The token standard of the NFT (e.g., ERC-721, ERC-1155)            |
| nft_token_id            | The ID of the NFT token                                            |
| nft_amount              | The amount of NFTs (only >1 for ERC1155)                           |
| currency_contract       | The address of the contract for the currency used in the NFT trade |
| currency_symbol         | The symbol of the currency used in the NFT trade                   |
| price                   | The full amount (ERC20) paid for the NFT, adjusted for decimals.   |
| price_usd               | The amount paid for the NFT, converted to USD                      |
| platform_fee_amount     | The amount (ERC20) paid in marketplace fees, adjusted for decimals |
| platform_fee_amount_usd | The amount paid in marketplace fees, converted to USD              |
| platform_fee_percentage | The percentage of the sale price charged as a marketplace fee      |
| platform_fee_address    | The address to which the platform fee was sent to (if applicable)  |
| royalty_fee_amount      | The amount (ERC20) paid in royalties, adjusted for decimals        |
| royalty_fee_amount_usd  | The amount paid in royalties, converted to USD                     |
| royalty_fee_percentage  | The percentage of the sale price paid as a royalty fee             |
| royalty_fee_address     | The address to which the royalty fee was sent to (if applicable)   |
| aggregator_address      | The address of the aggregator (if applicable)                      |
| aggregator_name         | The name of the aggregator (if applicable)                         |
| tx_from                 | The address that initiated the transaction                         |
| tx_to                   | The contract that received the transaction                         |

The data is also available in chain specific models:

- `nft_ethereum.trades`
- `nft_optimism.trades`
- ...
- 
### Contributing

#### Base marketplace models

Base table schema (to be implemented by marketplace models):

| Column Name              | Descriptions                                                               |
|--------------------------|----------------------------------------------------------------------------|
| **project**              | The name of the project/marketplace executing the trade                    |
| **project_version**      | The version of the project/marketplace executing the trade                 |
| **block_number**         | The number of the block in which the NFT trade occurred                    |
| **tx_hash**              | The hash of the transaction in which the NFT trade occurred                |
| **sub_tx_trade_id**      | A unique identifier of the NFT trade within one transaction                |
| trade_category           | The category of the NFT trade (buy, sell, other)                           |
| trade_type               | The type of NFT trade (primary/secondary)                                  |
| buyer                    | The address of the buyer                                                   |
| seller                   | The address of the seller                                                  |
| nft_contract_address     | The NFT contract address                                                   |
| nft_token_id             | The ID of the NFT token                                                    |
| nft_amount               | The amount of NFTs (only >1 for ERC1155)                                   |
| price_raw                | The raw full amount (ERC20) paid for the NFT, not adjusted for decimals    |
| currency_contract        | The address of the contract for the currency used in the NFT trade         |
| project_contract_address | The address of the contract for the project associated with the NFT trade  |
| platform_fee_amount_raw  | The raw amount (ERC20) paid in marketplace fees, not adjusted for decimals |
| platform_fee_address     | The address to which the platform fee was sent to (if applicable)          |
| royalty_fee_amount_raw   | The raw amount (ERC20) paid in royalties, not adjusted for decimals        |
| royalty_fee_address      | The address to which the royalty fee was sent to (if applicable)           |

Enrichment of these base tables is done in 1 model for each chain.
The enrichment logic can be found in `macros/models/sector/nft` and includes:

1. adding transaction information
2. adding NFT token information
3. adding ERC20 token information + handle ERC20 decimals
4. handle USD columns
5. adding aggregator columns
6. fixing buyer or seller for aggregator txs
7. calculating platform and royalty rates




### Migration

#### User changelog

The following changes affect the users:

##### models:
- [deprecated] `nft.events`, use `nft.trades` instead
- [deprecated] `nft.fees`, use `nft.trades` instead


##### columns:

- [new] `platform_fee_address`
- [new] `trade_type` (primary/secondary)
- [rename] `version` -> `project_version`
- [rename] `number_of_items` -> `nft_amount`
- [rename] `token_id` -> `nft_token_id`
- [rename] `token_standard` -> `nft_standard`
- [rename] `collection` -> `nft_collection` 
- [rename] `royalty_fee_receive_address` -> `royalty_fee_address`
- [rename] `amount_original` -> `price`
- [rename] `amount_raw` -> `price_raw`
- [rename] `amount_usd` -> `price_usd`
- [deprecated] `unique_trade_id`, replaced by combination of (project,project_version,block_number,tx_hash,sub_tx_trade_id)
- [deprecated] `evt_type` previously this was (trade/mint/burn), users can now leverage `trade_type` (primary/secondary)
- [deprecated] `royalty_fee_currency_symbol`, users can use `currency_symbol`



The migration for each protocol consists of 3 parts.
1. distilling the existing model to the base schema
2. validating the migration
2. purging the older model

#### Ethereum
| Platform    | Version | Trades (21/4/2023) | Migrated | Validated | Purged |
|-------------|:-------:|-------------------:|:--------:|:---------:|:------:|
| archipelago |   v1    |                561 |   [x]    |    [x]    |  [x]   |
| blur        |   v1    |          3,067,180 |   [x]    |    [x]    |  [x]   |
| cryptopunks |   v1    |             23,054 |   [x]    |    [x]    |  [x]   |
| element     |   v1    |            106,654 |   [x]    |    [x]    |  [x]   |
| foundation  |   v1    |            137,246 |   [x]    |    [x]    |  [x]   |
| looksrare   |   v1    |            401,647 |   [x]    |    [x]    |  [x]   |
| looksrare   |   v2    |              1,216 |   [x]    |    [x]    |  [x]   |
| opensea     |   v1    |         20,245,583 |          |           |        |
| opensea     |   v3    |         14,110,690 |          |           |        |
| opensea     |   v4    |          1,619,188 |          |           |        |
| sudoswap    |   v1    |            300,750 |   [x]    |    [x]    |  [x]   |
| superrare   |   v1    |             38,864 |   [x]    |    [x]    |  [x]   |
| x2y2        |   v1    |          1,843,487 |   [x]    |    [x]    |  [x]   |
| zora        |   v1    |              2,976 |   [x]    |    [x]    |  [x]   |
| zora        |   v2    |              3,491 |   [x]    |    [x]    |  [x]   |
| zora        |   v3    |              7,149 |   [x]    |    [x]    |  [x]   |
| trove       |         |                    |          |           |        |
| liquidifty  |         |                    |          |           |        |

#### Optimism

#### Arbitrum

#### Polygon

#### BNB

#### Solana
