{{
  config(
    
    alias='ccip_token_meta',
    materialized = 'view'
  )
}}

-- will need to be updated to add new fee tokens in the future

SELECT
   'base' AS blockchain,
   token_contract,
   token_symbol
FROM (VALUES
    (0x88Fb150BDc53A65fe94Dea0c9BA0a6dAf8C6e196, 'LINK'),
    (0x4200000000000000000000000000000000000006, 'WETH')
) a (token_contract, token_symbol)
