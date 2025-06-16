# Identify sector models that should be project

Our task is to review _sector level models in each dbt_subproject and identify if any should be project level and document our findings here. 

Sector level models that should be project are models where the sector level spell only contains data describing a single protocol. 


## daily_spellbook [5 of 5 processed] 
- Model: `geodnet_polygon_revenue.sql` (in _sector/depin/polygon/platforms/)
  Reason: Contains data exclusively for GeoDNet protocol on Polygon - tracks token burns and revenue for only this single protocol
  Recommended Change: Move to project level as `geodnet_polygon_revenue`

- Model: `friend_tech_base_base_trades.sql` (in _sector/social/base/)
  Reason: Contains data exclusively for Friend.tech protocol on Base - all trades are from FriendtechSharesV1 contract only
  Recommended Change: Move to project level as `friend_tech_base_trades`

- Model: `post_tech_arbitrum_base_trades.sql` (in _sector/social/arbitrum/)
  Reason: Contains data exclusively for Post.tech protocol on Arbitrum - all trades are from PostTechProfile contract only
  Recommended Change: Move to project level as `post_tech_arbitrum_trades`

- Model: `arrakis_finance_ethereum_trades.sql` (in _sector/alm/ethereum/platforms/)
  Reason: Contains data exclusively for Arrakis Finance protocol on Ethereum - uses only Arrakis-specific contracts and events
  Recommended Change: Move to project level as `arrakis_finance_ethereum_trades`

- Model: Multiple airdrop models (in _sector/airdrops/*/projects/)
  Reason: Each airdrop model contains data for only one protocol (e.g., cow_protocol_ethereum_airdrop_claims, hop_protocol_ethereum_airdrop_claims, etc.)
  Recommended Change: All single-protocol airdrop models should be moved to project level

- Model: Multiple referral reward models (in _sector/referral/rewards/platforms/)
  Reason: Each model contains data for only one protocol (e.g., mintfun_base_rewards, mintfun_ethereum_rewards, etc.)
  Recommended Change: Move all single-protocol referral models to project level

- Model: `cipher_arbitrum_base_trades.sql` (in _sector/social/arbitrum/)
  Reason: Contains data exclusively for Cipher protocol on Arbitrum - all trades are from Cipher_evt_Trade contract only
  Recommended Change: Move to project level as `cipher_arbitrum_trades`

- Model: `basepaint_base_rewards.sql` (in _sector/referral/rewards/platforms/)
  Reason: Contains data exclusively for Basepaint protocol on Base - tracks only rewards for this single protocol
  Recommended Change: Move to project level as `basepaint_base_rewards`

- Model: `slugs_optimism_rewards.sql` (in _sector/referral/rewards/platforms/)
  Reason: Contains data exclusively for Slugs protocol on Optimism - tracks only rewards for this single protocol
  Recommended Change: Move to project level as `slugs_optimism_rewards`


## hourly_spellbook [1 of 1 processed]
- Model: No single-protocol sector models found
  Reason: All sector models in hourly_spellbook aggregate data from multiple protocols (e.g., perpetual_trades.sql includes 20+ different protocols)
  Recommended Change: No changes needed - all sector models appropriately aggregate multiple protocols


## dex [1 of 1 processed]
- Model: No single-protocol sector models found
  Reason: The dex subproject doesn't have _sector directory structure - models are organized by function (trades, pools, etc.) and aggregate multiple DEX protocols
  Recommended Change: No changes needed - structure is appropriate for multi-protocol aggregation


## nft [1 of 1 processed]
- Model: No single-protocol sector models found
  Reason: All sector models aggregate data from multiple protocols (e.g., nft_aggregators.sql includes 13+ different blockchains/protocols)
  Recommended Change: No changes needed - all sector models appropriately aggregate multiple protocols


## tokens [1 of 1 processed]
- Model: No single-protocol sector models found
  Reason: The tokens subproject doesn't use _sector directory structure - models are organized by blockchain and token type, appropriately aggregating multiple protocols
  Recommended Change: No changes needed - structure is appropriate for multi-protocol aggregation


## solana [1 of 1 processed]
- Model: `hivemapper_solana_rewards.sql` (in hivemapper/ directory, not _sector/)
  Reason: Contains data exclusively for Hivemapper protocol on Solana - tracks only HONEY token transfers and rewards for this single protocol
  Recommended Change: Already correctly structured as project level (not in _sector directory)