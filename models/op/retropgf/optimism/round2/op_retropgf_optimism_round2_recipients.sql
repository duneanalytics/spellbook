{{ config(
        schema = 'op_retropgf_optimism'
        , alias='round2_recipients'
        , materialized='table'
  )
}}

{% set op_token = '0x4200000000000000000000000000000000000042' %}

with attestations as (
    SELECT *
    FROM {{ ref('optimism_attestationstation_optimism_events') }}
    
    WHERE from_hex(issuer) = '0x60c5c9c98bcbd0b0f2fd89b24c16e533baa8cda3'
    AND key IN ('retropgf.round-2.name','retropgf.round-2.award','retropgf.round-2.category')
    AND block_date BETWEEN cast('2023-03-30' as date) AND cast('2023-05-01' as date)
    )


SELECT 
    nm.block_date, nm.recipient, nm.issuer
    , nm.val AS recipient_name, ca.val AS recipient_category, aw.val AS award_amount
    , '{{op_token}}' AS award_token
    
    FROM (SELECT * FROM attestations where key = 'retropgf.round-2.name') nm
    LEFT JOIN (SELECT * FROM attestations where key = 'retropgf.round-2.award') aw
        ON aw.recipient = nm.recipient
    LEFT JOIN (SELECT * FROM attestations where key = 'retropgf.round-2.category') ca
        ON ca.recipient = nm.recipient