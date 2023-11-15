{{config(
        alias = 'bridges_base',
        post_hook='{{ expose_spells(\'["base"]\',
                                    "sector",
                                    "labels",
                                    \'["rantum"]\') }}')}}

SELECT blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
    ('base', 0x4200000000000000000000000000000000000010, 'Base', 'L2StandardBridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_base', 'identifier'),
    ('base', 0x4200000000000000000000000000000000000014, 'Base', 'L2ERC721Bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_base', 'identifier'),
    ('base', 0xaf54be5b6eec24d6bfacf1cce4eaf680a8239398, 'Stargate', 'Bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_base', 'identifier'),
    ('base', 0xe4edb277e41dc89ab076a1f049f4a3efa700bce8, 'Orbiter Finance', 'Bridge 2', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_base', 'identifier'),
    ('base', 0x80c67432656d59144ceff962e8faf8926599bcf8, 'Orbiter Finance', 'Bridge 1', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_base', 'identifier'),
    ('base', 0xd9d74a29307cc6fc8bf424ee4217f1a587fbc8dc, 'Orbiter Finance', 'Bridge 3', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_base', 'identifier'),
    ('base', 0x13e46b2a3f8512ed4682a8fb8b560589fe3c2172, 'Orbiter Finance', 'Bridge 4', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_base', 'identifier'),
    ('base', 0x45a318273749d6eb00f5f6ca3bc7cd3de26d642a, 'Owlto Finance', 'Bridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_base', 'identifier'),
    ('base', 0x00000000000007736e2f9aa5630b8c812e1f3fc9, 'ChainEye', 'MiniBridge', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_base', 'identifier'),
    ('base', 0x09aea4b2242abc8bb4bb78d537a67a245a7bec64, 'Across', 'SpokePoolVerifier', 'rantum', 'static', DATE '2023-11-15', now(), 'bridges_base', 'identifier')
    
    ) AS x (blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)