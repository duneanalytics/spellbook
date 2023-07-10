{{ config(
    schema='layerzero',
    alias = 'chain_list',
    tags = ['static']
    )
}}

WITH chain_list(chain_name, chain_id, endpoint_address) AS (
    values
    ('Ethereum', 101, '0x66a71dcef29a0ffbdbe3c6a460a3b5bc225cd675'),
    ('BNB Chain', 102, '0x3c2269811836af69497e5f486a85d7316753cf62'),
    ('Aptos', 108, '0x54ad3d30af77b60d939ae356e6606de9a4da67583f02b962d2d3f2e481484e90'),
    ('Avalanche', 106, '0x3c2269811836af69497e5f486a85d7316753cf62'),
    ('Polygon', 109, '0x3c2269811836af69497e5f486a85d7316753cf62'),
    ('Arbitrum', 110, '0x3c2269811836af69497e5f486a85d7316753cf62'),
    ('Optimism', 111, '0x3c2269811836af69497e5f486a85d7316753cf62'),
    ('Fantom', 112, '0xb6319cc6c8c27a8f5daf0dd3df91ea35c4720dd7'),
    ('Swimmer', 114, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'), -- deprecated from Official Endpoint Addresses
    ('DFK', 115, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Harmony', 116, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Moonbeam', 126, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Celo', 125, '0x3a73033c0b1407574c76bdbac67f126f6b4a9aa9'),
    ('Dexalot', 118, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Fuse', 138, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Gnosis', 145, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Klaytn', 150, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Metis', 151, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Intain', 152, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'), -- deprecated from Official Endpoint Addresses
    ('CoreDAO', 153, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('OKX', 155, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('Polygon zkEVM', 158, '0x9740ff91f1985d8d2b71494ae1a2f723bb3ed9e4'),
    ('zkSync Era', 165, '0x9b896c0e23220469c7ae69cb4bbae391eaa4c8da'),
    ('Moonriver', 167, '0x7004396c99d5690da76a7c59057c5f3a53e01704'),
    ('Canto', 159, '0x9740FF91F1985D8d2B71494aE1A2f723bb3Ed9E4'),
    ('Meter.io', 176, '0xa3a8e19253Ab400acDac1cB0eA36B88664D8DedF'),
    ('Sepolia', 161, '0x7cacBe439EaD55fa1c22790330b12835c6884a91'),
    ('Kava', 177, '0xb6319cC6c8c27A8F5dAF0dD3DF91EA35C4720dd7'),
    ('Arbitrum Nova', 175, '0x4EE2F9B7cf3A68966c370F3eb2C16613d3235245'),
    ('Tenet', 173, '0x2D61DCDD36F10b22176E0433B86F74567d529aAa')
)

SELECT *
FROM chain_list
