# Prices

This table dynamically constructs prices from `dex.trades` trading data.

* In comparison to `prices.usd` this greatly increases the amounts of assets that have a pricefeed.
* In contrast to `dex.view_token_prices` this table also contains data for hours where there was no trades.


## How it works

The script that generates this table is [insert_prices_from_dex_data.sql`](https://github.com/duneanalytics/abstractions/blob/master/ethereum/prices/insert_prices_from_dex_data.sql).

This script generates median hourly prices based on data from decentralized exchanges found in `dex.trades`.
It will assign asset prices based on a trading pair which has a pricefeed in `prices.usd`.

Let's take the $SPELL/ETH Pool for example.

- $ETH price is contained in `prices.usd`
- $SPELL price is not contained in `prices.usd`

In order to get the $SPELL price, the script will dynamically calculate the price of $SPELL based on the price of $ETH that was exchanged for it.

e.g. 5 $ETH were exchanged for 1,086,083 $SPELL.

Dex.trades will assign a `usd_amount` to this trade based on the $ETH price data in `prices.usd`.

That `usd_amount` is $23,498.

`5 * price of ETH (4.699,6) =  $23,498`

Calculating the price of $SPELL is now as simple as dividing the amount of tokens exchanged with the `usd_amount` recorded in `dex.trades`.

`$23,498/1,086,083 ≈ $0,02163`

We now have successfully calculated the price of 1 $SPELL.

In order to correct for extreme outliers and in order for this table to be performant the script then aggregates all recorded data into one `median_price` per hour.

## Known issues

In rare cases this script will generate pricefeeds that are based on illiquid pairs and therefore report wrong data.
This happens when all liquid trading pools of this token do not have a pricefeed in `prices.usd`.

An example of this would be $PLAY, a metaverse index from piedao.
The liquid trading pair for this asset is $PLAY/$DOUGH. The "correct" price of $PLAY is represented in this pool, but the combination of `dex.trades` and `prices.prices_from_dex_data` are not able to pick up this price.

Instead, `dex.trades` will only have a `usd_amount` for illiquid pairs of this asset. 
In this case, the $PLAY/$ETH pool has trades once in a while and these will have a `usd_amount` in `dex.trades`. The liquidity of the  $PLAY/$ETH pool is very low and it pretty much only consists of arbritrage trades.
Therefore, the resulting pricefeed in `prices.prices_from_dex_data` is faulty since it depends on the `usd_amount` in `dex.trades`.

In order to check for this, you should manually verify the results of `prices.prices_from_dex_data` in order to make sure arbritrage trades do not disturb the pricefeed constructed. A simple way of validating that the script is working with the right pools is checking the `sample_size` column. If the number seems suspiciously low, the script probably doesn't pick up the right price.

In cases like this, you have to manually construct a pricefeed.

## Outro

We are always looking to improve this table, if you have any ideas or comments don't hesistate to open a PR or contact us in our Discord.