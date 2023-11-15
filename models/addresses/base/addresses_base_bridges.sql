{{ config(alias = 'bridges',
        tags=['static'],
          ) }}

SELECT blockhain, address as address, bridge_name, description
FROM (VALUES 
  'base' as blockchain, (0x4200000000000000000000000000000000000010, 'Base', 'L2StandardBridge')
  ,'base' as blockchain, (0x4200000000000000000000000000000000000014, 'Base', 'L2ERC721Bridge')
  ,'base' as blockchain, (0xaf54be5b6eec24d6bfacf1cce4eaf680a8239398, 'Stargate', 'Bridge')
  ,'base' as blockchain, (0xe4edb277e41dc89ab076a1f049f4a3efa700bce8, 'Orbiter Finance', 'Bridge 2')
  ,'base' as blockchain, (0x80c67432656d59144ceff962e8faf8926599bcf8, 'Orbiter Finance', 'Bridge 1')
  ,'base' as blockchain, (0xd9d74a29307cc6fc8bf424ee4217f1a587fbc8dc, 'Orbiter Finance', 'Bridge 3')
  ,'base' as blockchain, (0x13e46b2a3f8512ed4682a8fb8b560589fe3c2172, 'Orbiter Finance', 'Bridge 4')
  ,'base' as blockchain, (0x45a318273749d6eb00f5f6ca3bc7cd3de26d642a, 'Owlto Finance', 'Bridge')
  ,'base' as blockchain, (0x00000000000007736e2f9aa5630b8c812e1f3fc9, 'ChainEye', 'MiniBridge')

  ) AS x (address, bridge_name, description)