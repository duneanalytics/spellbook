{{ config(
    alias = alias('tagging'),
    tags = ['dunesql', 'static'],
    unique_key = ['blockchain', 'tagging_method', 'identifier'])
}}

SELECT blockchain, tagging_method, identifier, protocol, protocol_type
FROM
(VALUES
    ('ethereum', 'zone', 0xf397619df7bfd4d1657ea9bdd9df7ff888731a11, 'OpenSea', 'Marketplace')
    , ('ethereum', 'zone', 0x9b814233894cd227f561b78cc65891aa55c62ad2, 'OpenSea', 'Marketplace') -- Royalties Distributor
    , ('ethereum', 'zone', 0x004c00500000ad104d7dbd00e3ae0a5c00560c00, 'OpenSea', 'Marketplace') -- Pausable Zone
    , ('ethereum', 'zone', 0x000000e7ec00e7b300774b00001314b8610022b8, 'OpenSea', 'Marketplace')
    , ('ethereum', 'zone', 0x110b2b128a9ed1be5ef3232d8e4e41640df5c2cd, 'OpenSea', 'Marketplace')
    , ('ethereum', 'tx_data_salt', 0x360c6ebe, 'OpenSea', 'Marketplace')
    , ('ethereum', 'zone', 0x0000000000d80cfcb8dfcd8b2c4fd9c813482938, 'Blur', 'Marketplace')
    , ('ethereum', 'tx_data_salt', 0x332d1229, 'Blur', 'Marketplace')
    , ('ethereum', 'fee_recipient', 0x1838de7d4e4e42c8eb7b204a91e28e9fad14f536, 'LooksRare', 'Marketplace')
    , ('ethereum', 'fee_recipient', 0x83Db44123E76503203fDf83D2bE58BE60c15B894, 'NFT Trader', 'OTC')
    , ('ethereum', 'fee_recipient', 0xa7673ab3b0949a0efcd818c86c71fff7cd645ac7, 'ENS.Vision', 'Marketplace')
    , ('ethereum', 'fee_recipient', 0xd54094d09109dD8D30340d3dfb66356C838a3b0c, 'Sound.xyz', 'Marketplace')
    , ('ethereum', 'fee_recipient', 0x3d279ac86c66f1b419046c287c0f1fab8e86efca, 'AlienSwap', 'Marketplace')
    , ('ethereum', 'fee_recipient', 0x7a8cdaa8c42b3242d832a95282e3a4363d5e3351, 'Alpha Sharks', 'Marketplace')
    , ('ethereum', 'tx_data_salt', 0xa8a9c101, 'Alpha Sharks', 'Marketplace')
    , ('ethereum', 'tx_data_salt', 0x64617461, 'Rarible', 'Marketplace')
    , ('ethereum', 'tx_data_salt', 0x61598d6d, 'Flip', 'Marketplace')
    , ('ethereum', 'fee_recipient', 0x75c168727666c8f2254c9f7561c53ba9c7b00681, 'Ordinals Market', 'Marketplace')
    , ('ethereum', 'tx_data_salt', 0x72db8c0b, 'Gem', 'Marketplace')
    , ('ethereum', 'tx_data_salt', 0x0021fb3f, 'Mint.fun', 'Marketplace')
    , ('ethereum', 'tx_data_salt', 0x1d4da48b, 'Reservoir', 'Marketplace')
    ) 
    x (blockchain, tagging_method, identifier, protocol, protocol_type)