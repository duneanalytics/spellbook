### Prices

#### Approximate Token Prices using DEX Trades
Since Optimism tokens can have different contract addresses than on Ethereum L1 and price feed providers may not have yet integrated these new token addresses, we can use on-chain DEX trades to approximate token prices. The core assumption made to approximate token prices is that three core stablecoins (USDC, DAI, USDT) prices are always equal to $1.

*Once official price feeds (i.e. CoinGecko, CoinPaprika) are live for Optimism, these tables can be deprecated and modified to match `prices.prices_from_dex_data` from the ethereum scema*

- Our first pass is to calculate all token prices which have been traded against stablecoins.

- Our second pass is to use the approximated ETH (WETH) price to approximate prices for tokens which may have only been traded against ETH.

- We then calculate prices for other "specialty" tokens (i.e. Synths from Synthetix vs sUSD, bridge liquidity tokens - i.e. hETH, nETH)

From these potential sources, we pick the price approximation which had the greatest number of samples as the canonical token price. If there are any ties, we rank again by 1) "Specialty" swaps (i.e. synths, bridge tokens), 2) Stablecoin swaps (since there's less variability due to ETH price), 3) ETH swaps.

- **prices.approx_prices_from_dex_data**: Base table schema for approximated trades by hour.

- **prices.insert_approx_prices_from_dex_data**: Script to insert hourly prices to the base table, using the logic referenced above.

- **prices.hourly_bridge_token_price_ratios**: Base table schema for the ratio of bridge tokens (i.e. hETH, nETH) to their underlying token (i.e. ETH). We use this as part of the prices.insert_approx_prices_from_dex_data logic

- **prices.insert_approx_prices_from_dex_data**: Script to insert hourly price ratios to the base table.


*Original Commit by Michael Silberling (@MSilb7 in Discord) - Please contact with any questions or improvements*
