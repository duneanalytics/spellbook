# Llama Aave Treasury Dashboard Tables

1. **Llama Aave Tokens:** Dynamically build a list of Aave's aTokens mapped to their underlying erc20 token with distinction on token type (i.e. deposit, variable borrow, stable borrow)
2. **Llama Aave Fees by Day:** Intermediary table summing up fees earned by the treasury per day due to events (i.e. flashloans, repayments). As of 12-30-21, only V1 takes fees (eventual todo - make this in raw transaction-level form).
3. **Llama Aave Daily Interest Rates**: Pulls daily interest rates per aToken
4. **Llama Aave Daily aToken Balances**: Pulls daily aToken balances across all addresses with compounding. This is sued to calculate Liquidity Mining APRs.
5. **Llama Aave Daily Treasury Events**: Final table storing daily treasury events. This should be used in end queries (remember to compound interest earned).

See Ethereum schema for more detailed documentation.
