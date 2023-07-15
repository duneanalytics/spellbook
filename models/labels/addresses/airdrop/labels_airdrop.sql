{{config(alias = alias('airdrop'),
        post_hook='{{ expose_spells(\'["optimism"]\',
                                    "sector",
                                    "labels",
                                    \'["ilemi"]\') }}'
)}}

SELECT * FROM {{ ref('labels_airdrop_1_receivers_optimism') }}