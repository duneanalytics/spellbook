{{ config(
        schema='lido_accounting_ethereum',
        alias = 'dai_referral_payment',

        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "lido_accounting",
                                \'["pipistrella", "adcv", "zergil1397"]\') }}'
        )
}}

with dai_referral_payments_addr AS (
    SELECT _recipient AS address FROM {{source('lido_ethereum','AllowedRecipientsRegistry_evt_RecipientAdded')}}
    UNION ALL
    SELECT 0xaf8aE6955d07776aB690e565Ba6Fbc79B8dE3a5d --rhino
),



dai_referral_payment_txns AS (
    SELECT  evt_block_time,
            evt_tx_hash,
            contract_address,
            value
    FROM  {{source('erc20_ethereum','evt_Transfer')}}
    WHERE "from" = 0x3e40D73EB977Dc6a537aF587D48316feE66E9C8c
    AND to IN (
        SELECT address FROM dai_referral_payments_addr
    )
    AND evt_block_time >= CAST('2023-01-01 00:00' AS TIMESTAMP)
    AND contract_address = 0x6B175474E89094C44Da98b954EedeAC495271d0F
    ORDER BY evt_block_time
)


    SELECT  evt_block_time as period,
            evt_tx_hash,
            contract_address AS token,
            value AS amount_token
    FROM dai_referral_payment_txns
