{{
    config(
        schema = 'balancer_v3_sonic',
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
    FROM {{ source('beethoven_x_v3_sonic','Vault_evt_LiquidityAddedToBuffer') }}  b
    WHERE b.amountUnderlying > 0
  ),

  -- Find ERC20 transfers in the same transactions to identify underlying tokens, make sure to match the amount too
  underlying_tokens AS (
    SELECT DISTINCT
      vm.vault_address,
      t.contract_address AS underlying_address,
      vm.evt_tx_hash
    FROM vault_mappings vm
    JOIN {{ source('erc20_sonic','evt_Transfer') }} t ON t.evt_tx_hash = vm.evt_tx_hash
      AND t.contract_address != vm.vault_address -- Exclude transfers of the vault token itself
      AND t.value = vm.amountUnderlying -- Match the amount transferred
  )

    SELECT DISTINCT
      'sonic' AS blockchain, 
      ut.vault_address as erc4626_token,
      COALESCE(vault_tokens.name, 'Unknown Vault') AS erc4626_token_name,
      COALESCE(vault_tokens.symbol, 'Unknown') AS erc4626_token_symbol,
      ut.underlying_address as underlying_token,
      COALESCE(underlying_tokens.symbol, 'Unknown') AS underlying_token_symbol,
      underlying_tokens.decimals AS decimals
    FROM underlying_tokens ut
    LEFT JOIN {{ source('tokens','erc20') }} vault_tokens ON vault_tokens.contract_address = ut.vault_address
      AND vault_tokens.blockchain = 'sonic'
    LEFT JOIN {{ source('tokens','erc20') }} underlying_tokens ON underlying_tokens.contract_address = ut.underlying_address
      AND underlying_tokens.blockchain = 'sonic'
 