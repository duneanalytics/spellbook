{{config(tags=['dunesql'],
        alias = alias('bridges_fantom'),
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "labels",
                                    \'["Henrystats"]\') }}')}}

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    -- Source: https://ftmscan.com/accounts/label/bridge
        ('fantom', 0xbbbd1bbb4f9b936c3604906d7592a644071de884, 'Allbridge: Bridge', 'bridge', 'Henrystats', 'static', DATE '2023-01-27' , now(), 'bridges_fantom', 'identifier')
    , ('fantom', 0x374b8a9f3ec5eb2d97eca84ea27aca45aa1c57ef, 'Celer Network: cBridge', 'bridge', 'Henrystats', 'static', DATE '2023-01-27' , now(), 'bridges_fantom', 'identifier')
    , ('fantom', 0x3795c36e7d12a8c252a20c5a7b455f7c57b60283, 'Celer Network: cBridge 2', 'bridge', 'Henrystats', 'static', DATE '2023-01-27' , now(), 'bridges_fantom', 'identifier')
    , ('fantom', 0x43de2d77bf8027e25dbd179b491e8d64f38398aa, 'deBridgeGate', 'bridge', 'Henrystats', 'static', DATE '2023-01-27' , now(), 'bridges_fantom', 'identifier')
    , ('fantom', 0x7ab4c5738e39e613186affd4c50dbfdff6c22065, 'DEUS Finance: Bridge', 'bridge', 'Henrystats', 'static', DATE '2023-01-27' , now(), 'bridges_fantom', 'identifier')
    , ('fantom', 0xb6319cc6c8c27a8f5daf0dd3df91ea35c4720dd7, 'LayerZero: Fantom Endpoint', 'bridge', 'Henrystats', 'static', DATE '2023-01-27' , now(), 'bridges_fantom', 'identifier')
    , ('fantom', 0xc10ef9f491c9b59f936957026020c321651ac078, 'Multichain: anyCall V6', 'bridge', 'Henrystats', 'static', DATE '2023-01-27' , now(), 'bridges_fantom', 'identifier')
    , ('fantom', 0xaf41a65f786339e7911f4acdad6bd49426f2dc6b, 'Synapse: Bridge', 'bridge', 'Henrystats', 'static', DATE '2023-01-27' , now() , 'bridges_fantom', 'identifier')
    , ('fantom', 0x7bc05ff03397950e8dee098b354c37f449907c20, 'Synapse: Bridge Zap', 'bridge', 'Henrystats', 'static', DATE '2023-01-27' , now(), 'bridges_fantom', 'identifier')
    , ('fantom', 0x31efc4aeaa7c39e54a33fdc3c46ee2bd70ae0a09, 'xPollinate: Transaction Manager', 'bridge', 'Henrystats', 'static', DATE '2023-01-27' , now(), 'bridges_fantom', 'identifier')
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)