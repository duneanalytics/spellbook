{{
    config(
        schema = 'nft_solana'
        , tags = ['dunesql']
        , alias = alias('trades')
        ,materialized = 'incremental'
        ,file_format = 'delta'
        ,incremental_strategy = 'merge'
        ,unique_key = ['unique_trade_id','block_slot']
        ,post_hook='{{ expose_spells(\'["solana"]\',
                                    "sector",
                                    "nft",
                                    \'["ilemi"]\') }}'
    )
}}

{% set solana_marketplaces = [
    ref('magiceden_solana_trades')
    , ref('tensorswap_solana_trades')
] %}


{% for marketplace in solana_marketplaces %}
SELECT
    *
FROM {{ marketplace }}

UNION ALL

{% endfor %}