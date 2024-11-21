{{
  config(
    
    alias='ccip_token_meta',
    materialized = 'view'
  )
}}

-- will need to be updated to add new fee tokens in the future

SELECT
   'polygon' AS blockchain,
   token_contract,
   token_symbol
FROM (VALUES
    (0xb0897686c545045aFc77CF20eC7A532E3120E0F1, 'LINK'),
    (0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270, 'WMATIC')
) a (token_contract, token_symbol)
