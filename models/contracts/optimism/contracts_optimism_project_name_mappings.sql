{{ 
  config(
    alias='project_name_mappings',
    unique_key='dune_name',
    post_hook='{{ expose_spells(\'["optimism"]\',
                              "sector",
                              "contracts",
                              \'["msilb7", "chuxin"]\') }}'
    )  
}}

select 
  dune_name
  ,mapped_name
from (
    values
    ('lyra_v1',	'Lyra')
    ,('Lyra V1', 'Lyra')
    ,('aave_v3', 'Aave')
    ,('perp_v2', 'Perpetual Protocol')
    ,('synthetix_futures', 'Kwenta')
    ,('zeroex', '0x')
    ,('uniswap_v3', 'Uniswap V3')
    ,('Uniswap V3', 'Uniswap V3')
    ,('oneinch', '1inch')
    ,('pika_perp_v2', 'Pika Protocol')
    ,('quixotic_v1', 'Quix')
    ,('quixotic_v2', 'Quix')
    ,('quixotic_v3', 'Quix')
    ,('quixotic_v4', 'Quix')
    ,('across_v2', 'Across')
    ,('openocean_v2', 'OpenOcean')
    ,('setprotocol_v2',	'Set Protocol')
    ,('kromatikafinance', 'Kromatika')
    ,('kratosdao', 'Kratos Dao')
    ,('curvefi', 'Curve')
    ,('pika_perp', 'Pika Protocol')
    ,('dhedge_v2', 'Dhedge')
    ,('bitbtc', 'Bitbtc Protocol')
    ,('teleportr', 'Teleportr/ Warp Speed')
    ,('balancer_v2', 'Beethoven X')
    ,('stargate', 'Stargate Finance')
    ,('quixotic_v5', 'Quix')
    ,('lyra_avalon', 'Lyra')
    ,('Lyra', 'Avalon Lyra')
    ,('Unlock', 'Unlock Protocol')
    ,('Xy Finance', 'XY Finance')
    ,('Qidao', 'QiDao')
    ,('Defisaver', 'Defi Saver')
    ,('Layerzero', 'Layer Zero')
    ,('Xtoken', 'xToken')
    ,('Instadapp', 'InstaDapp')
    ,('Lifi', 'LiFi')
    ,('Optimistic Explorer', 'Optimistic Explorer - Get Started NFT')
    ,('ironbank', 'Iron Bank')
    ,('iron_bank', 'Iron Bank')
    ,('bluesweep', 'BlueSweep')
    ,('hidden_hand', 'Hidden Hand')
    ,('quixotic', 'Quix')
    ,('project galaxy', 'Galxe')
    ,('project_galaxy', 'Galxe')
    ,('Masoud_ecc', 'ECC Domains')
    ,('opx_finance', 'OPX Finance')
    ,('pooltogether_v3', 'PoolTogether')
    ,('beethovenx', 'Beethoven X')
    ,('openxswap', 'OpenXSwap')
    ,('eccdomains', 'ECC Domains')
    ,('2pi_network','2Pi Network')
    ,('twopi_network','2Pi Network')
    ,('acryptos', 'AcryptoS')
    ,('woofi', 'WooFi')
    ,('powerbomb_finance','Powerbomb Finance')
    ,('powerbomb','Powerbomb Finance')
    ,('lemma_finance','Lemma Finance')
    ,('lemma','Lemma Finance')
    ) as temp_table (dune_name, mapped_name)
