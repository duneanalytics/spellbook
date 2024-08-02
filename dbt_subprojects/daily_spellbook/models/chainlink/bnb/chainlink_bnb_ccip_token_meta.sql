{{
  config(
    
    alias='ccip_token_meta',
    materialized = 'view'
  )
}}

-- will need to be updated to add new fee tokens in the future

SELECT
   'bnb' AS blockchain,
   token_contract,
   token_symbol
FROM (VALUES
    (0x404460C6A5EdE2D891e8297795264fDe62ADBB75, 'LINK'),
    (0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c, 'WBNB')
) a (token_contract, token_symbol)
