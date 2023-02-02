{% docs uniswap_v3_arbitrum_trades %}

## Uniswap V3 trades on Arbitrum

## Introduction
This is a model that standardizes the data for Uniswap v3 data on the Arbitrum blockchain. 
It is written in SparkSQL and used to standardize the data to make it easier for analysts to run queries.

On a high level, the model joins prices, token information and pool information to all trades and standardizes the data so it can be inserted in dex.trades upstream.

## Data Standardization Pipeline
The data standardization pipeline performs the following operations:

Queries all Uniswap V3 Pools on Arbitrum using the Pair_evt_Swap table
Joins the UniswapV3Factory_evt_PoolCreated table to get token0 and token1 addresses which are needed for further joins
Adds the blockchain name (Arbitrum), project name (Uniswap), and version (3) to the results
Adds the block date to partition the data
Adds the symbol of token0 and token1 to the results
Orders the symbols of token0 and token1 alphabetically and concatenates them with a dash to create a consistent token_pair
Calculates the display amount of token0 and token1 that was bought/sold
Calculates the amount in USD by either using the value of amount_usd (if available), or by multiplying the raw amount with the token's price.
Table Description
The following table lists the columns and their descriptions in the resulting standardized data table:



{% enddocs %}