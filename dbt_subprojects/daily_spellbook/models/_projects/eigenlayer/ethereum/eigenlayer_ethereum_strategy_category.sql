{{
    config(
        schema = 'eigenlayer_ethereum',
        alias = 'strategy_category',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "eigenlayer",
                                    \'["bowenli"]\') }}'
    )
}}

select
  strategy,
  category,
  name
from (
values
  (0x93c4b944d05dfe6df7645a86cd2206016c51564d,'eth lst','stETH'),
  (0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2,'eth lst','rETH'),
  (0x54945180db7943c0ed0fee7edab2bd24620256bc,'eth lst','cbETH'),
  (0x9d7ed45ee2e8fc5482fa2428f15c971e6369011d,'eth lst','ETHx'),
  (0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff,'eth lst','ankrETH'),
  (0xa4c637e0f704745d182e4d38cab7e7485321d059,'eth lst','oETH'),
  (0x57ba429517c3473b6d34ca9acd56c0e735b94c02,'eth lst','osETH'),
  (0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6,'eth lst','swETH'),
  (0x7ca911e83dabf90c90dd3de5411a10f1a6112184,'eth lst','wBETH'),
  (0x8ca7a5d6f3acd3a7a8bc468a8cd0fb14b6bd28b6,'eth lst','sfrxETH'),
  (0xae60d8180437b5c34bb956822ac2710972584473,'eth lst','lsETH'),
  (0x298afb19a105d59e74658c4c334ff360bade6dd2,'eth lst','mETH'),
  (0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0,'eth native','ETH'),
  (0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7,'eigen','EIGEN')
) t (strategy, category, name)
