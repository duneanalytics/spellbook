# Set Protocol Abstractions

This folder contains all of the heavy duty abstractions used to track Set components, TVL, etc. For now, the only abstraction that requires a dedicated cron job is the component-level price feed.

## Daily Component Prices

This table assembles daily prices for all Set components that we can find, preferentially using the coinpaprika feed
from `prices.usd`, and then looking into dex trades from `prices.prices_from_dex_data`. Since a lot of low liquidity tokens
have massive price spikes, we've taken a more conservative approach to working with `prices.prices_from_dex_data` and erred on the side
of throwing out data that has less certainty. For any given day, we only use that day's trade data if the total sample size is > 5 and if
there is data across at least 5 different hours that day. Then, we take the median price as the day's average price.

Another thing we do is that many of our Sets use components that don't have active trading but are closely pegged to other tokens, most
notably Aave Interest Bearing tokens. For these tokens, we map the price directly from the underlying, e.g. we map `astETH`'s directly to
`stETH`. We're still working on trying to get price feeds for Compound's tokens.