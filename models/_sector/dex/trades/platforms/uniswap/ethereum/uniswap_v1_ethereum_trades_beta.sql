{{ config(
    tags = ['dunesql'],
    schema = 'uniswap_v1_ethereum',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "uniswap_v1",
                                \'["jeff-dude", "markusbkoch", "masquot", "milkyklim", "0xBoxer", "mewwts", "hagaetc"]\') }}'
    )
}}

select *
from {{ ref('dex_trades_beta') }}
where project = 'uniswap'
  and version = '1'
  and blockchain = 'ethereum'