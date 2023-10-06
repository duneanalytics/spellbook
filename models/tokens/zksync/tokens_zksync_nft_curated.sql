{{ config( alias = alias('nft_curated'), tags=['static', 'dunesql']) }}

SELECT contract_address, name, symbol
FROM (VALUES
-- ERC-721:
        (0xd07180c423f9b8cf84012aa28cc174f3c433ee29, 'LIBERTAS OMNIBUS', 'LO'),
        (0xcc788c0495894c01f01cd328cf637c7c441ee69e, 'zkSync Name Service', 'ZNS'),
        (0x068dc5a04ad0e6babd6b4782b8d13c2fb4107bda, 'zkSync Name Service', 'ZKNS'),
        (0x53ec17bd635f7a54b3551e76fd53db8881028fc3, 'Mint Square Storefront', 'MINTSQ'),
        (0xd29aa7bdd3cbb32557973dad995a3219d307721f, 'CitizenID', 'TEVAN'),
        (0x50b2b7092bcc15fbb8ac74fe9796cf24602897ad, 'ReformistSphinx', 'SPHINX'),
        (0xd43a183c97db9174962607a8b6552ce320eac5aa, 'l2telegraph.xyz', 'l2t'),
        (0x6dd28c2c5b91dd63b4d4e78ecac7139878371768, 'Merkly ONFT', 'MERK'),
        (0x112e5059a4742ad8b2baf9c453fda8695c200454, 'Karat Network Claimer', 'KATC'),
        (0x86c011f4dd7e3018b96adab6b3ef5716f881d440, 'Ape Into zkSummer with Across & Taho', 'zkSummerAcrossTaho'),
        (0x1ec43b024a1c8d084bcfeb2c0548b6661c528dfa, 'PixelCollectorNft', 'PIXEL'),
        (0x4a26b21db75bc1a91928bb50b8133c2da8aad83b, 'SyncSwap x Galxe', 'SyncSwap_with_Galxe'),
        (0x08e33286ddbc5f0bc6e4d7b77f4e5d081b26f27a, 'Karat VC Registry', 'KAVC'),
        (0xfd54762d435a490405dda0fbc92b7168934e8525, 'Maverick Position NFT', 'MPN'),
        (0x5fff818e3ee4a63412fc953030aa7024451fbdfd, 'Tabi Mermaid Pearl', 'TMP'),
        (0x936c9a1b8f88bfdbd5066ad08e5d773bc82eb15f, 'iZiSwap Liquidity NFT', 'IZISWAP-LIQUIDITY-NFT'),
        (0x483fde31bce3dcc168e23a870831b50ce2ccd1f1, 'iZiSwap Liquidity NFT', 'IZISWAP-LIQUIDITY-NFT'),
        (0xf27e53edc24be11b4c5dc4631fd75ea0ed896d64, 'Robots.Farm Airdrop', 'RFI'),
        (0xa815e2ed7f7d5b0c49fda367f249232a1b9d2883, 'Pancake V3 Positions NFT-V1', 'PCS-V3-POS'),
-- ERC-1155:
        (0x3f9931144300f5feada137d7cfe74faaa7ef6497, 'Commemorative OG Cards', 'CMOGC'),
        (0x089b353642e6f066bad44a6a854ef4e3bcb0dc9c, 'Carv Events', 'CARV-EVNT'),
        (0x54948af9d4220acee7aa5340c818865f6b313f96, 'Allowlist Pass', 'CMAP'),
        (0xb1adbdd8cfd39cedb08b88dc0c2e4c9112718f7e, 'Archiswap Alpha One Certificate', 'AAOC'),
        (0x72d0531b02bea8997c962995828ba39f985a7282, 'Club3 SBT', 'CLUB3'),
        (0xe2452d204235accb8b29e18df0f8c1d1f67e7c3c, 'The Element', 'AHE'),
        (0xe0e9e2f208eb5c953345526bcb515120128298cf, 'The Animal Age Props', 'TAAP')
  ) AS temp_table (contract_address, name, symbol)
