{{
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'strategy_category',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}',
        materialized = 'table'
    )
}}

select
  strategy,
  token,
  category,
  name
from (
values
  (0x93c4b944d05dfe6df7645a86cd2206016c51564d, 0xae7ab96520de3a18e5e111b5eaab095312d7fe84, 'eth lst','stETH'),
  (0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2, 0xae78736cd615f374d3085123a210448e74fc6393, 'eth lst','rETH'),
  (0x54945180db7943c0ed0fee7edab2bd24620256bc, 0xbe9895146f7af43049ca1c1ae358b0541ea49704, 'eth lst','cbETH'),
  (0x9d7ed45ee2e8fc5482fa2428f15c971e6369011d, 0xa35b1b31ce002fbf2058d22f30f95d405200a15b, 'eth lst','ETHx'),
  (0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff, 0xe95a203b1a91a908f9b9ce46459d101078c2c3cb, 'eth lst','ankrETH'),
  (0xa4c637e0f704745d182e4d38cab7e7485321d059, 0x856c4efb76c1d1ae02e20ceb03a2a6a08b0b8dc3, 'eth lst','oETH'),
  (0x57ba429517c3473b6d34ca9acd56c0e735b94c02, 0xf1c9acdc66974dfb6decb12aa385b9cd01190e38, 'eth lst','osETH'),
  (0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6, 0xf951e335afb289353dc249e82926178eac7ded78, 'eth lst','swETH'),
  (0x7ca911e83dabf90c90dd3de5411a10f1a6112184, 0xa2e3356610840701bdf5611a53974510ae27e2e1, 'eth lst','wBETH'),
  (0x8ca7a5d6f3acd3a7a8bc468a8cd0fb14b6bd28b6, 0xac3e018457b222d93114458476f3e3416abbe38f, 'eth lst','sfrxETH'),
  (0xae60d8180437b5c34bb956822ac2710972584473, 0x8c1bed5b9a0928467c9b1341da1d7bd5e10b6549, 'eth lst','lsETH'),
  (0x298afb19a105d59e74658c4c334ff360bade6dd2, 0xd5f7838f5c461feff7fe49ea5ebaf7728bb0adfa, 'eth lst','mETH'),
  -- use WETH for native restaked ETH
  (0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 'eth native','ETH'),
  (0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7, 0x83e9115d334d248ce39a6f36144aeab5b3456e75, 'eigen','EIGEN')
) t (strategy, token, category, name)
