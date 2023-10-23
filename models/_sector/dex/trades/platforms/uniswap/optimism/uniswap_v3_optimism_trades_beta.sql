{{ config(
    tags = ['dunesql'],
    schema = 'uniswap_v3_optimism',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "uniswap_v3",
                                \'["jeff-dude", "markusbkoch", "masquot", "milkyklim", "0xBoxer", "mewwts", "hagaetc"]\') }}'
    )
}}

select *
from {{ ref('dex_trades_beta') }}
where project = 'uniswap'
  and version = '3'
  and blockchain = 'optimism'