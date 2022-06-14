# Aave Tables

1. **Aave Tokens:** Dynamically build a list of Aave's aTokens mapped to their underlying erc20 token with distinction on token type (i.e. deposit, variable borrow, stable borrow) and program (i.e. Aave, Aave ARC, RWA).
2. **Aave Daily aToken Balances**: Pulls daily aToken balances across all addresses with compounding. This is used to calculate Liquidity Mining APRs.
3. **Aave Daily Interest Rates**: Pulls daily interest rates per aToken
4. **Aave Liquidity Mining Rates**: Pulls daily liquidity mining rates per aToken (uses the aToken balances table).
