# Notes for users of `nft.trades`

This repo collects trade data of NFT marketplaces in a single table `nft.trades`.  

The goal is to enable a powerful new wave of Dune dashboards on NFTs, and inspire new contributors to add more marketplaces and NFT data. 

ERC721 and ERC1155 `transfer` data is integrated for every trade transaction to enable analysis of trades consisting of multiple NFTs.

Known limitations:
- OpenSea and Rarible support non-ETH trades. Some of the tokens used for these transactions are not in Dune's `prices.usd`. In these cases, the `usd_amount` is `NULL`. At the time of writing (July 2021), for OpenSea and Rarible, the percentage of rows with missing USD values is 2% or less.
- In some cases, there are rounding errors on the `original_amount` fields.


# Notes for contributors to `nft.trades`

Here are some things to check before doing your pull request:
- As much as possible, verify data from the NFT page on the market place itself as well as on Etherscan. In one case, we noticed that event log data was not correctly decoded in case the trade was done in WETH instead of ETH.
- Verify buyer and seller. In some scenario's (again when currency is not ETH) these get inversed. A good way to double-check is to verify against ERC721 transfers.
- Different types of trades ("Direct Purchase", "Offer Accepted", "Auction Settled"...)  often have a different encoding in the event logs.

Don't hesitate to ask for help in Discord if you get stuck: https://discord.gg/ppntYkQu


# Credits

Amazing pioneer work on NFTs by the following Dune users:
- https://duneanalytics.com/rchen8
- https://duneanalytics.com/keeganead

The following power users helped me in various ways along the way: 
- https://duneanalytics.com/0xBoxer
- https://duneanalytics.com/danner_eth
