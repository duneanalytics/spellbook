{{ 
  config(
    tags = ['static'],
    alias = 'project_name_mappings',
    unique_key='dune_name',
    post_hook='{{ expose_spells(\'["zksync"]\',
                              "sector",
                              "contracts",
                              \'["lgingerich"]\') }}'
    )  
}}

select 
  dune_name
  , mapped_name
from (
    values
     ('4everland', '4EVERLAND')
    ,('across_v2', 'Across')
    ,('dappgate_onft', 'DappGate ONFT')
    ,('daura', 'daura')
    ,('dottery', 'Dottery')
    ,('dracula_finance', 'Dracula Finance')
    ,('element_ex', 'Element Market')
    ,('eralend', 'EraLend')
    ,('era_name_service', 'Era Name Service')
    ,('gambit', 'Gambit')
    ,('gemswap', 'GemSwap')
    ,('gnosis_safe', 'Safe')
    ,('goal3', 'Goal3')
    ,('guildxyz', 'Guild')
    ,('hs_defuture', 'Holdstation Defutures')
    ,('izumi_finance', 'iZUMi Finance')
    ,('kreator_land', 'Kreatorland')
    ,('maverick_v1', 'Maverick Protocol')
    ,('merkly', 'Merkly')
    ,('mint_square', 'Mint Square')
    ,('mute', 'Mute')
    ,('odos_protocol', 'ODOS')
    ,('oneinch', '1inch')
    ,('overnight_finance', 'Overnight Finance')
    ,('pancakeswap_v2', 'PancakeSwap')
    ,('pancakeswap_v3', 'PancakeSwap')
    ,('parax', 'ParaX')
    ,('plexus', 'PLEXUS')
    ,('pudgy_penguins', 'Pudgy Penguins')
    ,('rabbithole', 'RabbitHole')
    ,('reactorfusion', 'ReactorFusion')
    ,('rhino_fi', 'rhino.fi')
    ,('rollup_finance', 'Rollup Finance')
    ,('satori', 'Satori Finance')
    ,('secondlive', 'SecondLive')
    ,('socket_v2', 'Socket')
    ,('spacefi_io', 'SpaceFi')
    ,('syncswap', 'SyncSwap')
    ,('teva_market', 'Tevaera')
    ,('uniswap_v3', 'Uniswap')
    ,('velocore', 'Velocore')
    ,('velocore_v0', 'Velocore')
    ,('velocore_v2', 'Velocore')
    ,('zkape', 'zkApe')
    ,('zkape_nft', 'zkApe NFT')
    ,('zksync_era', 'zkSync Era')

    ) as temp_table (dune_name, mapped_name)