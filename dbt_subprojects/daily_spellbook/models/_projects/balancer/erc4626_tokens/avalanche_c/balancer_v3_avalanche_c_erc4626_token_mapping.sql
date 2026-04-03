{{
    config(
        schema = 'balancer_v3_avalanche_c',
        alias = 'erc4626_token_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

-- Only ops team adds liquidity to buffer, so we can use these events to find vaults and amounts
WITH vault_mappings AS (
    SELECT DISTINCT
      evt_tx_hash,
      wrappedToken AS vault_address,
      amountUnderlying,
      amountWrapped
    FROM {{ source('balancer_v3_avalanche_c', 'Vault_evt_LiquidityAddedToBuffer') }} b
    WHERE b.amountUnderlying > 0
  ),

  -- Find ERC20 transfers in the same transactions to identify underlying tokens, make sure to match the amount too
  underlying_tokens AS (
    SELECT DISTINCT
      vm.vault_address,
      t.contract_address AS underlying_address,
      vm.evt_tx_hash
    FROM vault_mappings vm
    JOIN {{ source('erc20_avalanche_c', 'evt_Transfer') }} t ON t.evt_tx_hash = vm.evt_tx_hash
      AND t.contract_address != vm.vault_address
      AND t.value = vm.amountUnderlying
      AND t.to = 0xba1333333333a1ba1108e8412f11850a5c319ba9
  )

SELECT DISTINCT
  'avalanche_c' AS blockchain, 
  ut.vault_address AS erc4626_token,
  COALESCE(vault_tokens.name, 'Unknown Vault') AS erc4626_token_name,
  COALESCE(vault_tokens.symbol, 'Unknown') AS erc4626_token_symbol,
  ut.underlying_address AS underlying_token,
  COALESCE(underlying_tokens.symbol, 'Unknown') AS underlying_token_symbol,
  vault_tokens.decimals AS decimals
FROM underlying_tokens ut
LEFT JOIN {{ source('tokens', 'erc20') }} vault_tokens ON vault_tokens.contract_address = ut.vault_address
  AND vault_tokens.blockchain = 'avalanche_c'
LEFT JOIN {{ source('tokens', 'erc20') }} underlying_tokens ON underlying_tokens.contract_address = ut.underlying_address
  AND underlying_tokens.blockchain = 'avalanche_c'
