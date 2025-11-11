{{ config(
    schema = 'bridges',
    alias = 'layerzero_chain_indexes',
    materialized = 'view',
    )
}}

-- source: https://metadata.layerzero-api.com/v1/metadata/deployments

SELECT blockchain, chain_id, endpoint_address
    FROM (VALUES
    ('ethereum', 101, 0x66a71dcef29a0ffbdbe3c6a460a3b5bc225cd675),
    ('bnb', 102, 0x3c2269811836af69497e5f486a85d7316753cf62),
    ('avalanch_c', 106, 0x3c2269811836af69497e5f486a85d7316753cf62),
    ('aptos', 108, 0x54ad3d30af77b60d939ae356e6606de9a4da67583f02b962d2d3f2e481484e90),
    ('polygon', 109, 0x3c2269811836af69497e5f486a85d7316753cf62),
    ('arbitrum', 110, 0x3c2269811836af69497e5f486a85d7316753cf62),
    ('optimism', 111, 0x3c2269811836af69497e5f486a85d7316753cf62),
    ('fantom', 112, 0xb6319cc6c8c27a8f5daf0dd3df91ea35c4720dd7),
    ('swimmer', 114, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('dfk', 115, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('harmony', 116, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('dexalot', 118, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('celo', 125, 0x3a73033c0b1407574c76bdbac67f126f6b4a9aa9),
    ('moonbeam', 126, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('fuse', 138, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('gnosis', 145, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('klaytn', 150, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('metis', 151, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('intain', 152, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('coredao', 153, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('okx', 155, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('zkevm', 158, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('canto', 159, 0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4),
    ('sepolia', 161, 0x7cacbe439ead55fa1c22790330b12835c6884a91),
    ('zksync', 165, 0x9b896c0e23220469c7ae69cb4bbae391eaa4c8da),
    ('moonriver', 167, 0x7004396c99d5690da76a7c59057c5f3a53e01704),
    ('tenet', 173, 0x2d61dcdd36f10b22176e0433b86f74567d529aaa),
    ('nova', 175, 0x4ee2f9b7cf3a68966c370f3eb2c16613d3235245),
    ('meter', 176, 0xa3a8e19253ab400acdac1cb0ea36b88664d8dedf),
    ('kava', 177, 0xb6319cc6c8c27a8f5daf0dd3df91ea35c4720dd7),
    ('mantle', 181, 0xb6319cc6c8c27a8f5daf0dd3df91ea35c4720dd7),
    ('base', 184, 0xb6319cc6c8c27a8f5daf0dd3df91ea35c4720dd7),
    ('xlayer', 274, 0xb6319cc6c8c27a8f5daf0dd3df91ea35c4720dd7),
    ('mantle-testnet', 10181, 0x2ca20802fd1fd9649ba8aa7e50f0c82b479f35fe),
    ('kite-testnet', 10415, 0x83c73da98cf733b03315afa8758834b36a195b87)
    ) AS x (blockchain, chain_id, endpoint_address)
