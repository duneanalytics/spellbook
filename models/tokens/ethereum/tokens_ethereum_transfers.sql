{{ config(
        alias = 'transfers',
        tags=['dunesql'],
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "tokens",
                                    \'["hildobby", "aalan3"]\') }}'
        )
}}


{{transfers_enrich(
    blockchain='ethereum',
    transfers_base = ref('tokens_ethereum_transfers_base'),
)}}
