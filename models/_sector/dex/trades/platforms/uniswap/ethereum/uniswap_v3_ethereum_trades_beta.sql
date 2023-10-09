{{ config(
    tags = ['dunesql'],
    schema = 'uniswap_v3_ethereum',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "uniswap_v3",
                                \'["jeff-dude", "markusbkoch", "masquot", "milkyklim", "0xBoxer", "mewwts", "hagaetc"]\') }}'
    )
}}

-- demo of not breaking existing queries on `uniswap_v3_ethereum.trades`
-- uniswap_v3_ethereum.base_trades -> dex.trades -> uniswap_v3_ethereum.trades
select *
from {{ ref('dex_trades_beta') }}
where project = 'uniswap'
  and version = '3'
  and blockchain = 'ethereum'