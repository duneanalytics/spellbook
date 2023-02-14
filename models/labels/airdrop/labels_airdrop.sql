{{config(alias='nft',
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "labels",
                                    \'["ilemi"]\') }}'
)}}

SELECT * FROM {{ ref('airdrop_1_receivers_optimism') }}