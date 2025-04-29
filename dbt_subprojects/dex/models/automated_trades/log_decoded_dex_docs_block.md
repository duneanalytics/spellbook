{% docs log_decoded_dex_trades_doc %}

## Table Description

The `dex.trades` table captures detailed data on trades executed via decentralized exchanges (DEXs). This table captures all trade events that happen across different liqudity sources. 

## Functional Overview

The `dex.trades` table provides an in-depth view of trades on decentralized exchanges like uniswap or curve. This table includes entries for each segment of a trade that passes through different liquidity pools, as well as single-step trades. For example, a user may initiate a trade to swap USDC for PEPE. If this trade is executed through multiple liquidity pools, such as USDC-WETH and WETH-PEPE, the `dex.trades` table will record each segment of the trade as a separate entry. Conversely, a single-step trade, such as directly swapping USDC for ETH, will be recorded as a single entry.

This detailed approach allows for granular analysis of trade execution paths, enabling users to:

- **Analyze Liquidity Sources**: Understand which liquidity pools are used and how they interact in both single-step and multi-step trades.
- **Track Trade Execution Paths**: Follow the exact route a trade takes across different DEXs and liquidity pools.
- **Assess Slippage and Execution Quality**: Evaluate the impact of each step on the overall trade execution, including slippage and price changes.
- **Monitor Market Dynamics**: Gain insights into the behavior and dynamics of different liquidity pools and DEXs over time.

By providing comprehensive trade details, the `dex.trades` table supports advanced analytics and research into DEX trading behavior and liquidity management.

Complimentary tables include `dex_aggregator.trades`, in which trade-intents that are routed through aggregators are recorded. The volume routed through aggregators is also recorded in the dex.trades table, one row in dex_aggregator trades corresponds to one or more rows in dex.trades.

{% enddocs %}