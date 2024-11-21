{{ config(
        schema = 'lifi',
        alias = 'trades',
        post_hook='{{ expose_spells(blockchains = \'["fantom", "optimism"]\',
                                    spell_type = "project",
                                    spell_name = "lifi",
                                    contributors = \'["Henrystats", "hosuke"]\') }}'
        )
}}

SELECT *
FROM {{ ref('dex_aggregator_trades') }}
WHERE project = 'lifi'