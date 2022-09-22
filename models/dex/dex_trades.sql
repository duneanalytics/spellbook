{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "dex",
                                \'["jeff-dude", "bh2smith"]\') }}'
        )
}}

SELECT "AMM" as category, * from {{ ref('dex_amm_trades') }}
UNION
SELECT "Aggregator" as category, * from {{ ref('dex_aggregator_trades') }}
