{{
  config(
    
    alias='ccip_token_meta',
    materialized = 'view'
  )
}}

-- will need to be updated to add new fee tokens in the future

SELECT
   'optimism' AS blockchain,
   token_contract,
   token_symbol
FROM (VALUES
    (0x350a791Bfc2C21F9Ed5d10980Dad2e2638ffa7f6, 'LINK'),
    (0x4200000000000000000000000000000000000006, 'WETH')
) a (token_contract, token_symbol)
