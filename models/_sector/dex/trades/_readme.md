{% docs uniswap_v3_arbitrum_trades %}

## Uniswap V3 trades on Arbitrum

### Conceptual Summary
This model transforms the trade events from Uniswap v3 pool contracts on the Arbitrum network into a table that is easier to work with. The transformed data includes information about the tokens traded, the amounts of tokens bought and sold, and the USD value of the trade. The model also adds metadata about the tokens, such as their symbol and decimals.

### Data Sources
The following data sources are used in this model:

1. [uniswap_v3_arbitrum.Pair_evt_Swap](/#!/source/source.spellbook.uniswap_v3_arbitrum.Pair_evt_Swap) : contains the Swap events emitted when a trade occurs on a Uniswap v3 pool
2. [uniswap_v3_arbitrum.UniswapV3Factory_evt_PoolCreated](/#!/source/source.spellbook.uniswap_v3_arbitrum.UniswapV3Factory_evt_PoolCreated) : contains information about the pool contract
3. [tokens.erc20](/#!/source/source.spellbook.tokens_erc20) : contains the symbol and decimals of token0 and token1
4. [prices.usd](/#!/source/source.spellbook.prices.usd) : contains the price of token0 and token1 in USD
5. [arbitrum.transactions](/#!/source/source.spellbook.arbitrum.transactions) : contains the transaction hash, block_number and tx_from of the transaction that emitted the Swap event

### Data Transformations
The contract_address of the pool contract in the Swap event is used to join with the ``uniswap_v3_arbitrum.UniswapV3Factory_evt_PoolCreated`` event to obtain the contract addresses of token0 and token1.  
The contract addresses of token0 and token1 are used to join with the ``tokens.erc20`` table to obtain the symbol and decimals of the tokens.  
The decimals of token0 and token1 are used to calculate the display amount of tokens bought and sold.  
The contract_address of token0 and token1 are used to join with the ``prices.usd`` table to obtain the price of the tokens in USD.  
The amounts of token0 and token1 that were bought and sold are used to calculate the USD value of the trade.  
The taker in the Swap event is not necessarily the trader that bought and sold token0 and token1. The trader can potentially be derived from the EOA (Externally Owned Account) that signed the transaction.  

{% enddocs %}


{% docs uniswap_arbitrum_trades %}
## Uniswap trades on Arbitrum

This model unions the data of different protocol versions. However, currently only Uniswap V3 is deployed on Arbitrum.

### models involved

- [uniswap_v3_arbitrum_trades](/#!/model/model.spellbook.uniswap_v3_arbitrum_trades)

{% enddocs %}