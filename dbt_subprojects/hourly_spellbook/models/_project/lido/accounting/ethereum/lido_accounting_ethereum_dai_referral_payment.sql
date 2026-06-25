{{ config(
        schema='lido_accounting_ethereum',
        alias = 'dai_referral_payment',

        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'append'
        , post_hook='{{ hide_spells() }}'
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
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
    ORDER BY evt_block_time
)


    SELECT  evt_block_time as period,
            evt_tx_hash,
            contract_address AS token,
            value AS amount_token
    FROM dai_referral_payment_txns d
    {% if is_incremental() %}
    -- append-only dedup: drop rows already inserted by a previous run inside the
    -- incremental window (no event index in the output, so a merge unique_key would
    -- collapse legitimately duplicated transfers within one tx)
    WHERE not exists (
        select 1
        from {{ this }} t
        where t.period = d.evt_block_time
          and t.evt_tx_hash = d.evt_tx_hash
          and t.token = d.contract_address
          and t.amount_token = d.value
    )
    {% endif %}
