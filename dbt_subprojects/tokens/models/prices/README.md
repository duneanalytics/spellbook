## Overview

- Pricing data is pulled from a 3rd party: [Coinpaprika](https://coinpaprika.com/)
  - The API can be accessed [here](https://api.coinpaprika.com/v1/coins/) -- this returns all tokens available
  - To filter down the API results, provide unique token ID value at end of URL
  - To find a token ID, follow example below

## Example
- I want to add Uniswap token on the Ethereum blockchain to `prices.usd` for historical pricing data
- Find Uniswap token on Coinpaprika: https://coinpaprika.com/coin/uni-uniswap/
- Find the API ID in the overview section, slightly down the page: `uni-uniswap`
- Test the API call result in my web browser: https://api.coinpaprika.com/v1/coins/uni-uniswap
- Ensure the token feed via the API is live, with the `"is_active":true` flag
- Add a row to the Ethereum prices spell: https://github.com/duneanalytics/spellbook/blob/main/dbt_subprojects/tokens/models/prices/ethereum/prices_ethereum_tokens.sql
  - In the PR process to add a token, tests will run to ensure unique address value
  - Tests against the API will also run, to ensure results return as expected
- Request review from maintainers to get PR merged
- PR is approved and merged
- After a few days, the backend pipeline to ingest the new data will be live in `prices.usd`

## What is the goal of the prices spells?
- `prices.tokens`
  - Spell manually maintained by the Spellbook community, which contains all token metadata necessary to pull pricing information for given token(s)
  - Tells a backend pipeline which token price data to fetch, including full history and ongoing price data
  - Output of price data fetched from API feeds to `prices.usd`
- `prices.usd`
  - Due to a background pipeline ingesting the source price data, this table is considered a source in Spellbook
  - Contains all the historical price data for all tokens within `prices.tokens`

The goal within Spellbook is to tell `prices.usd` which tokens to fetch historical price data for, across a variety of blockchains. `prices.usd` is then used heavily downstream in many spells as a source, to generate `amount_usd` data for variety of analysis.

## Who helps maintain and build out prices spells?
The great Spellbook community has been on top of this, continually adding tokens to existing chains & adding new prices spells for new chains on Dune. The Dune team maintaining Spellbook will look to help keep these up-to-date and provide tooling to simplify the process.

## When can prices be added?
Any time! Dune team will do their best to merge as quickly as possible to get live pricing data for relevant tokens.

## FAQ
- My PR to add a new token on Ethereum blockchain just got approved and merged, but I don't see the data in `prices.usd` yet? Why is that?
  - Once a PR is merged, that only adds the token to `prices.tokens` spell. This spell then tells a background pipeline to go obtain pricing data for that new token. This data ingestion pipeline to read the historical data from the API can take some time. We have noticed it can take multiple days to backfill. Please be patient and check back after multiple days.
- The GH action tied to my PR for adding prices is failing, why is that?
  - This can be tied to a few different reasons:
    - The blockchain isn't supported by the 3rd party vendor, validate [here](https://api.coinpaprika.com/v1/contracts)
    - The `token_id` value provided either can't be found (i.e. incorrect ID) or is no longer active
  - Sometimes, we are able to continue on even with a failure in CI pipelines attached to PR
    - If a token is cross-chain with the same address, but a blockchain isn't supported, the data can be pulled from the other blockchain -- be sure to keep blockchain consistent with the spell it's in, even if pulling from different one on the API
- What if my token can't be found on Coinpaprika?
  - Please follow their process to add tokens [here](https://coinpaprika.com/add/)
  - If request is urgent, Dune team may be able to expedite request once it's made in above link
