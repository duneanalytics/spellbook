{{ config(
    tags = ['dunesql'],
    schema = 'defiswap_ethereum',
    alias = 'trades_beta',
    materialized = 'view',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "defiswap",
                                    \'["pandajackson42", "hosuke"]\') }}'
    )
}}

select *
from {{ ref('dex_trades_beta') }}
where project = 'defiswap'
  and blockchain = 'ethereum'