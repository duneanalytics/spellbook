{{
  config(
    
    alias='ccip_token_meta',
    materialized = 'view'
  )
}}

-- will need to be updated to add new fee tokens in the future

SELECT
   'avalanche_c' AS blockchain,
   token_contract,
   token_symbol
FROM (VALUES
    (0x5947BB275c521040051D82396192181b413227A3, 'LINK'),
    (0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7, 'WAVAX')
) a (token_contract, token_symbol)
