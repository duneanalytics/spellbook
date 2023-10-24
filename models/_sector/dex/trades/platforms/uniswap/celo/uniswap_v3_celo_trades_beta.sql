{{ config(
    tags = ['dunesql'],
    schema = 'uniswap_v3_celo',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["celo"]\',
                                "project",
                                "uniswap_v3",
                                \'["jeff-dude", "markusbkoch", "masquot", "milkyklim", "0xBoxer", "mewwts", "hagaetc", "tomfutago"]\') }}'
    )
}}

select *
from {{ ref('dex_trades_beta') }}
where project = 'uniswap'
  and version = '3'
  and blockchain = 'celo'