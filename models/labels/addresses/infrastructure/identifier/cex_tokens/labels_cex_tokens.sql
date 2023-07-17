{{config(
    alias = alias('cex_tokens'),
    tags=['static'],
    post_hook='{{ expose_spells(\'["ethereum", "bnb", "polygon", "solana", "arbitrum", "optimism", "fantom", "avalanche_c", "gnosis"]\',
                                "sector",
                                "labels",
                                \'["hildobby"]\') }}'
)}}

SELECT blockchain, lower(address) as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM
(   
    -- Sources:
    -- https://www.coingecko.com/en/categories/centralized-exchange-token-cex

    VALUES
    ('ethereum', '0xb8c77482e45f1f44de1745f52c74426c631bdd52', 'BNB (Binance)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x2af5d2ad76741191d15dfe7bf6ac92d4bd912ca3', 'LEO (Bitfinex)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x75231f58b43240c9718dd58b4967c5114342a86c', 'OKB (OKX)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b', 'CRO (Crypto.com)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xe66747a101bff2dba3697199dcce5b743b454759', 'GT (Gate)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x6f259637dcd74c767781e37bc6133cd6a68aa161', 'HT (Huobi)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x925206b8a707096ed26ae47c84747fe0bb734f59', 'WBT (WhiteBIT)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x667102bd3413bfeaa3dffb48fa8288819e480a88', 'TKX (Tokenize Xchange)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x4691937a7508860f876c9c0a2a617e7d9e945d4b', 'WOO (WOO Network)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('bnb', '0x4691937a7508860f876c9c0a2a617e7d9e945d4b', 'WOO (WOO Network)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('arbitrum', '0xcafcd85d8ca7ad1e1c6f82f651fa15e33aefd07b', 'WOO (WOO Network)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('polygon', '0x1b815d120b3ef02039ee11dc2d33de7aa4a8c603', 'WOO (WOO Network)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('avalanche_c', '0xabc9547b534519ff73921b1fba6e672b5f58d083', 'WOO (WOO Network)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('fantom', '0x6626c47c00f1d87902fc13eecfac3ed06d5e8d8a', 'WOO (WOO Network)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('solana', 'E5rk3nmgLUuKUiS94gg4bpWwWwyjCMtddsAXkTFLtHEy', 'WOO (WOO Network)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x11eef04c884e24d9b7b4760e7476d06ddf797f36', 'MX (MEXC Global)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xfcf8eda095e37a41e002e266daad7efc1579bc0a', 'FLEX (CoinFLEX)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xf34960d9d60be18cc1d5afc1a6f012a723a28811', 'KCS (Kucoin)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x19de6b897ed14a376dda0fe53a5420d2ac828a28', 'BGB (Bitget)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xb113c6cf239f60d380359b762e95c13817275277', 'BMEX (BitMEX)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xff742d05420b6aca4481f635ad8341f81a6300c2', 'ASD (AscendEx)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xbd0793332e9fb844a52a205a233ef27a5b34b927', 'ZB (ZB)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x986ee2b944c42d017f52af21c4c69b84dbea35d8', 'BMX (BitMart)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x72dd4b6bd852a3aa172be4d6c5a6dbec588cf131', 'NGC (NAGAX)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x618e75ac90b12c6049ba3b27f5d5f8651b0037f6', 'QASH (Liquid Global)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xab93df617f51e1e415b5b4f8111f122d6b48e55c', 'DETO (Delta Exchange)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('bnb', '0xd32d01a43c869edcd1117c640fbdcfcfd97d9d65', 'NMX (Nominex)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xaa602de53347579f86b996d2add74bb6f79462b2', 'ZMT (Zipmex)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x9b39a0b97319a9bd5fed217c1db7b030453bac91', 'TCH (CoinTiger)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x6be61833fc4381990e82d7d4a9f4c9b3f67ea941', 'HBT (Hotbit)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xe50365f5d679cb98a1dd62d6f6e58e59321bcddf', 'LA (LATOKEN)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x83869de76b9ad8125e22b857f519f001588c0f62', 'EXM (EXMO)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x7be00ed6796b21656732e8f739fc1b8f1c53da0d', 'ACXT (ACDX Exchange)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x1b073382e63411e3bcffe90ac1b9a43fefa1ec6f', 'BEST (Bitpanda)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x50d1c9771902476076ecfc8b2a83ad6b9355a4c9', 'FTT (FTX)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('solana', 'AGFEad2et2ZJif9jaGpdMixQqvW5i81aBdvKe7PHNfz3', 'FTT (FTX)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xe7976c4efc60d9f4c200cc1bcef1a1e3b02c73e7', 'MAX (MAX Exchange)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x5b71bee9d961b1b848f8485eec8d8787f80217f5', 'BF (Bitforex)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x946551dd05c5abd7cc808927480225ce36d8c475', 'ONE (BigONE)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('bnb', '0x04baf95fd4c52fd09a56d840baee0ab8d7357bf0', 'ONE (BigONE)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xa2a54f1ec1f09316ef12c1770d32ed8f21b1fb6a', 'DFT (DigiFinex)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x7707aada3ce7722ac63b91727daf1999849f6835', 'BNK (Bankera)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x93b1e78a3e652cd2e71c4a767595b77282344932', 'BITO (BitoPro)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('bnb', '0xbfff3571f9fd637ae7cfb63ac2112fd18264ce62', 'TARM (Tarmex)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0x1df7aa5551e801e280007dc0fc0454e2d06c1a89', 'BKK (BKEX)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('ethereum', '0xd4f6f9ae14399fd5eb8dfc7725f0094a1a7f5d80', 'BST (Bitsten)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    , ('bnb', '0x1d89272821b3acc245acc1794e79a07d13c3e7e7', 'BST (Bitsten)', 'infrastructure', 'hildobby', 'static', timestamp('2023-03-15'), now(), 'cex_tokens', 'identifier')
    ) AS temp_table (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)
;