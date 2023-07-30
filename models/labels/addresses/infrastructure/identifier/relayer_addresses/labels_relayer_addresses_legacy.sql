{{config(
    alias = alias('relayer_addresses',legacy_model=True),
    tags=['legacy'],
    post_hook='{{ expose_spells(\'["ethereum", "bnb", "polygon", "arbitrum", "optimism", "fantom", "avalanche_c", "gnosis"]\',
                                "sector",
                                "labels",
                                \'["msilb7"]\') }}'
)}}

SELECT 1 as address, 1 as blockchain