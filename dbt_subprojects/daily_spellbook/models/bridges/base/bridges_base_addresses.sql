{{ config(alias = 'addresses',
        schema = 'bridges_base',
        tags=['static']
          ) }}

SELECT blockchain, address, bridge_name, description
FROM (VALUES 
  ('base', 0x4200000000000000000000000000000000000010, 'Base', 'L2StandardBridge')
  ,('base', 0x4200000000000000000000000000000000000014, 'Base', 'L2ERC721Bridge')
  ,('base', 0xaf54be5b6eec24d6bfacf1cce4eaf680a8239398, 'Stargate', 'Bridge')
  ,('base', 0xe4edb277e41dc89ab076a1f049f4a3efa700bce8, 'Orbiter Finance', 'Bridge 2')
  ,('base', 0x80c67432656d59144ceff962e8faf8926599bcf8, 'Orbiter Finance', 'Bridge 1')
  ,('base', 0xd9d74a29307cc6fc8bf424ee4217f1a587fbc8dc, 'Orbiter Finance', 'Bridge 3')
  ,('base', 0x13e46b2a3f8512ed4682a8fb8b560589fe3c2172, 'Orbiter Finance', 'Bridge 4')
  ,('base', 0x45a318273749d6eb00f5f6ca3bc7cd3de26d642a, 'Owlto Finance', 'Bridge')
  ,('base', 0x00000000000007736e2f9aa5630b8c812e1f3fc9, 'ChainEye', 'MiniBridge')
  ,('base', 0x09aea4b2242abc8bb4bb78d537a67a245a7bec64, 'Across', 'SpokePoolVerifier')

  ) AS x (blockchain, address, bridge_name, description)