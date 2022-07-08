### Prices

#### Approximate Token Prices using DEX Trades
Since Optimism tokens can have different contract addresses than on Ethereum L1 and price feed providers may not have yet integrated these new token addresses, we can use on-chain DEX trades to approximate token prices. The core assumption made to approximate token prices is that three core stablecoins (USDC, DAI, USDT) prices are always equal to $1.

*Once official price feeds (i.e. CoinGecko, CoinPaprika) are live for Optimism, these tables can be deprecated and modified to match `prices.prices_from_dex_data` from the ethereum scema*

- Our first pass is to pull token prices from Chainlink price feeds, and populate `usd_amount` in dex.trades for trades including the underlying token.

- Our second pass is to use the `usd_amount` to approximate prices for tokens traded against the tokens with Chainlink price feeds.

- Our third pass is to re-populate `usd_amount` with our new known token prices, and then calculate the prices for tokens which have been traded our new known set (i.e. tokens only traded against tokens which are traded against tokens with Chainlink price feeds - LUSD <> sUSD <> ETH)


- **prices.approx_prices_from_dex_data**: Base table schema for approximated trades by hour.

- **prices.insert_approx_prices_from_dex_data**: Script to insert hourly prices to the base table, using the logic referenced above.

- **prices.hourly_bridge_token_price_ratios**: Base table schema for the ratio of bridge tokens (i.e. hETH, nETH) to their underlying token (i.e. ETH). We use this as part of the prices.insert_approx_prices_from_dex_data logic

- **prices.insert_approx_prices_from_dex_data**: Script to insert hourly price ratios to the base table.


*Original Commit by Michael Silberling (@MSilb7 in Discord) - Please contact with any questions or improvements*
