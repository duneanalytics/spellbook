{% docs labels_nft_smart_trader_roi_eth %}

The model itself is fundamentally an ROI-based ranking of NFT traders. The ranking of ROI is based on “realized” ROI, which is defined as ((ETH Gained from sales of NFTs) / (ETH Spent on buying those NFTS)) - 1. This could have easily been calculated from nft.trades with one or two CTEs, but the length of the query was intended to cover many edge cases that would make a trader not as “smart”. The query makes some opinionated decisions about what kind of traders to be filtered out, namely I filter for wallets:

1. that have sold at least 10 NFTs
2. that have traded at least 3 different collections
3. that have spent at least 1 ETH on all purchases
4. have a positive overall ROI (including unsold NFTs based on floor price)

The first 2 filters are to make sure people didnt just get lucky on one or two trades and have had success with multiple collections. The third is the filter out extremely high ROIs from low absolute numbers. The last one is an interesting filter I came up with only after looking at the data. It seems like many addresses have a high “realized” ROI only because they haven’t sold many of the other NFTs they purchased that have dropped significantly in price. Therefore I used the floor prices (from a combination of reservoir and `nft_ethereum.collection_stats` (to get both on and off-chain floor data) to estimate their return on their unsold assets. This way we ensure that they at least have a breakeven return when including the unsold portion of their portfolio. 

Finally I ranked the remaining traders based on their “realized” ROI and added labels for the top 1st/2nd/3rd percentiles among them. I chose to only include the top 3% since including the top 10% would result in ~14,000 addresses which is not as useful of a group. The ~5k addresses in the top 3% are more than sufficient and is a more manageable number for someone looking to use this for further segmentation / analysis. 

The shortcomings here are that there is no period of time included in the ROI calculation. A further improvement could be to annualize the returns and/or restrict the set of addresses to only those that have been trading for at least x number of weeks. I included columns that would make this possible in the intermediate CTEs used in the queries.

{% enddocs %}
