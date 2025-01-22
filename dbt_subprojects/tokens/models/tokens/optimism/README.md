# Optimism Tokens

To manually add a token, add it to `tokens_optimism_erc20_curated.sql`

Files:
- `tokens_optimism_erc20_curated.sql`: Manually configured table
  - token_type: What best describes this token? Is it a vault or LP token, or a lowest-level underlying token?
    - underlying: This is the rawest form of the token (i.e. USDC, DAI, WETH, OP, UNI, GTC) - Counted in On-Chain Value
    - receipt: This is a vault/LP receipt token (i.e. HOP-LP-USDC, aUSDC, LP Tokens) - NOT Counted in On-Chain Value (double count)
    - na: This is a placeholder token that does not have a price (i.e. virtual tokens)

- `tokens_optimism_erc20_bridged_mapping.sql`: Automatically pick up tokens from the L1->L2 Bridge. If the token is known on L1, we pull in the decimals.

- `tokens_optimism_erc20_generated.sql`: Aggregate of protocol-generated tokens (i.e. deposit receipt tokens)

- `tokens_optimism_erc20_transfer_source.sql`: Pull of all contracts which emitted an erc20 transfer event.

- `tokens_optimism_erc20`: The final aggregate table - Shows all ERC20 token addresses with mapped symbols and decimals when known.

- `tokens_optimism_erc20_stablecoins.sql`: Manually curated reference table for stablecoins.