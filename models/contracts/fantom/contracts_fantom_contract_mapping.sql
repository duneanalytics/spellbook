{{
  config(
        alias='contract_mapping',
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "contracts",
                                    \'["davidcheongyl"]\') }}'
        )
}}

select
  trim(lower(contract_address)) as contract_address
  ,trim(project_name) as project_name
  ,trim(project_type) as project_type
from
(
  values
  ('0xDEF189DeAEF76E379df891899eb5A00a94cBC250', '0x Exchange', 'Aggregator'),
  ('0xf760c5b88d970d6f97e64e264dac5a3767dafd74', '0x Exchange', 'Aggregator'),
  ('0xb4d961671cadfed687e040b076eee29840c142e5', '0x Exchange', 'Aggregator'),
  ('0xca64d4225804f2ae069760cb5ff2f1d8bac1c2f9', '0x Exchange', 'Aggregator'),
  ('0x47f01db18a38261e4cb153bae6db7d3743acb33c', '0x Exchange', 'Aggregator'),
  ('0xd2d638aa6ff4694a9147f2c8b09e86f569fa052b', 'Alpaca Finance', 'DeFi'),
  ('0x838b7f64fa89d322c563a6f904851a13a164f84c', 'Alpaca Finance', 'DeFi'),
  ('0x2d5408f2287bf9f9b05404794459a846651d0a59', 'Alpaca Finance', 'DeFi'),
  ('0xe1d2ca01bc88f325ff7266dd2165944f3caf0d3d', 'Alpaca Finance', 'DeFi'),
  ('0xa625ab01b08ce023b2a342dbb12a16f2c8489a8f', 'Alpaca Finance', 'DeFi'),
  ('0xcab46f1397fdf4b0f31cef05345141e51f877edb', 'Alpaca Finance', 'DeFi'),
  ('0xafd1eb025bf584a11112a3fc790bd715ed9a98b1', 'Alpaca Finance', 'DeFi'),
  ('0xe5009dd5912a68b0d7c6f874cd0b4492c9f0e5cd', 'Based Finance', 'DeFi'),
  ('0x9ec66b9409d4cd8d4a4c90950ff0fd26bb39ad84', 'Based Finance', 'DeFi'),
  ('0xac0fa95058616d7539b6eecb6418a68e7c18a746', 'Based Finance', 'DeFi'),
  ('0x08101f0152a21313234edb1962e1181fb0f26e90', 'Beethoven X', 'Dex Aggregator'),
  ('0xf26d401c8051c664c1d2047f0cbc34105ac68cd0', 'Beethoven X', 'Dex Aggregator'),
  ('0x3be3a2cd1d02f273c28e452e45bcd7c92b2d2e0b', 'Beethoven X', 'Dex Aggregator'),
  ('0x5d0a9c5aac3024b1100b0f76c2c17a13453f3f33', 'Beethoven X', 'Dex Aggregator'),
  ('0x3122e55914638cf162bea3c96632b64caa5516d5', 'Beethoven X', 'Dex Aggregator'),
  ('0xbbbd1bbb4f9b936c3604906d7592a644071de884', 'Allbridge', 'Bridge'),
  ('0x374b8a9f3ec5eb2d97eca84ea27aca45aa1c57ef', 'Celer Network', 'Bridge'),
  ('0x3795c36e7d12a8c252a20c5a7b455f7c57b60283', 'Celer Network', 'Bridge'),
  ('0x43de2d77bf8027e25dbd179b491e8d64f38398aa', 'Debridge', 'Bridge'),
  ('0x7ab4c5738e39e613186affd4c50dbfdff6c22065', 'Deus Finance', 'Bridge'),
  ('0xb6319cc6c8c27a8f5daf0dd3df91ea35c4720dd7', 'LayerZero', 'Bridge'),
  ('0xc10ef9f491c9b59f936957026020c321651ac078', 'Multichain', 'Bridge'),
  ('0xaf41a65f786339e7911f4acdad6bd49426f2dc6b', 'Synapse', 'Bridge'),
  ('0x7bc05ff03397950e8dee098b354c37f449907c20', 'Synapse', 'Bridge'),
  ('0x5ab6215ab8344c28b899efde93bee47b124200fb', 'Market Protocol', 'DeFi'),
  ('0xfd000ddcea75a2e23059881c3589f6425bff1abb', 'Paintswap', 'Dex'),
  ('0x16327e3fbdaca3bcf7e38f5af2599d2ddc33ae52', 'Spritswap', 'Dex'),
  ('0x31efc4aeaa7c39e54a33fdc3c46ee2bd70ae0a09', 'xPollinate', 'Dex'),
  ('0x71c7656ec7ab88b098defb751b7401b5f6d8976f', 'FTMscan', 'Donations'),
  ('0xccdb6c5818116a2a93ee6ec28cf92445d56c623e', 'Nova', 'Donations'),
  ('0x5ec86d4d826bf3e12ee2486b9df01d7cfa99b6ca', 'Rarity Game', 'Donations'),
  ('0x8E1701CFd85258DDb8DFE89Bc4c7350822B9601D', 'MEXC', 'Cex'),

) as temp_table (contract_address,project_name,project_type)
;