{{ config(
        alias = alias('transfers'),
        tags=['dunesql'],
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "fungible",
                                    \'["hildobby", "aalan3"]\') }}'
        )
}}


{{transfers_enrich(
    blockchain='ethereum',
    transfers_base = ref('tokens_ethereum_transfers_base'),
)}}
