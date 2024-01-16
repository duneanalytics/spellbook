{{
  config(
    
    alias='ccip_token_meta',
    materialized = 'view'
  )
}}

-- will need to be updated to add new fee tokens in the future

SELECT
   'arbitrum' AS blockchain,
   token_contract,
   token_symbol
FROM (VALUES
    (0xf97f4df75117a78c1A5a0DBb814Af92458539FB4, 'LINK'),
    (0x82aF49447D8a07e3bd95BD0d56f35241523fBab1, 'WETH')
) a (token_contract, token_symbol)
