{{ config(schema = 'tokens_candle',
        alias='nft'
        )
}}

SELECT
  contract_address, name, symbol, standard
FROM
  (VALUES
  (CAST('0x22c1f6050e56d2876009903609a2cc3fef83b415' AS string),CAST('POAP' AS string),CAST('The Proof of Attendance Protocol' AS string),CAST('erc721' AS string)),
  (CAST('0x35E681D4Af615BDbcb0aeC2bdFf13A5096F14673' AS string),CAST('CandleTreasury' AS string),CAST('CT' AS string),CAST('erc721' AS string)),
  (CAST('0xf149f7f8a5af5db86908eaf4490207306b7c6266' AS string),CAST("Movement DAOs Ascended Ape Collection from Mainnet" AS string),CAST('MAPE-1420' AS string),CAST('erc721' AS string)),
  (CAST('0x75f525f8c9bc57bf8099b6658750cfdd795177ff' AS string),CAST("Alecs NFTs" AS string),CAST('' AS string),CAST('erc721' AS string))
  ) as temp_table (contract_address, name, symbol, standard)
