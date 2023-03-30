{{ config(
        schema = 'op_retropgf_optimism'
        , alias='round2_recipients'
        , materialized='table'
        , unique_key = ['schedule_confirmed_date', 'schedule_start_date']
        , post_hook='{{ expose_spells(\'["optimism"]\',
                                  "project",
                                  "op_retropgf",
                                  \'["msilb7"]\') }}'
  )
}}

with attestations as (
    SELECT *
    FROM {{ ref('optimism_attestationstation_optimism_events') }}
    
    WHERE from_hex(issuer) = '0x60c5c9c98bcbd0b0f2fd89b24c16e533baa8cda3'
    AND key IN ('retropgf.round-2.name','retropgf.round-2.award','retropgf.round-2.category')
    )


SELECT 
    nm.block_date, nm.recipient, nm.issuer, nm.val AS recipient_name, ca.val AS recipient_category, aw.val AS award_amount
    
    FROM (SELECT * FROM attestations where key = 'retropgf.round-2.name') nm
    LEFT JOIN (SELECT * FROM attestations where key = 'retropgf.round-2.award') aw
        ON aw.recipient = nm.recipient
    LEFT JOIN (SELECT * FROM attestations where key = 'retropgf.round-2.category') ca
        ON ca.recipient = nm.recipient