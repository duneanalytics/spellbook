{{ config(alias = 'nft_curated', tags=['static']) }}

SELECT
  contract_address, name, symbol
FROM
  (VALUES
   (0x1FC10ef15E041C5D3C54042e52EB0C54CB9b710c,	'Base is for Builders', 'BASEBUILDERS')
  ,(0x061A883E8c2FEFFB4F3eA42046ABD4bE88E1333f, 'Omnichain Adventures (Part 2)',  'OMNIA2')
  ,(0x5b51Cf49Cb48617084eF35e7c7d7A21914769ff1, 'Frenpet NFT',  'Frenpet')
  ,(0xF882c982a95F4D3e8187eFE12713835406d11840, 'Merkly ONFT',  'MERK')
  ,(0x36a358b3Ba1FB368E35b71ea40c7f4Ab89bFd8e1, 'l2telegraph.xyz', 'l2t')
  ,(0x29D24B2AC84E51F842fb8c1533CD972eb83c65Ce, 'L2Marathon', 'MarathonRunner')

) as temp_table (contract_address, name, symbol)
