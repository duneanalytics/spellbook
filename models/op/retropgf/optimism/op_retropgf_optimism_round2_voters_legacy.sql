{{ config(
        schema = 'op_retropgf_optimism'
        , alias = alias('round2_voters', legacy_model=True)
        , materialized='table'
        , tags=['legacy', 'static']
  )
}}

with attestations as (
    SELECT *
        --replace invisible characters
        , REGEXP_REPLACE(key, '[[:cntrl:]]', '') AS key_mapped
    FROM {{ ref('optimism_attestationstation_optimism_events_legacy') }}
    
    WHERE issuer = '0x60c5c9c98bcbd0b0f2fd89b24c16e533baa8cda3'
    AND REGEXP_REPLACE(key, '[[:cntrl:]]', '') = 'retropgf.round-2.can-vote'
    AND block_date BETWEEN cast('2023-02-01' as date) AND cast('2023-04-01' as date)
    )


SELECT 
    v.block_date, v.recipient AS voter, v.issuer, v.val_string AS can_vote
    
    FROM (SELECT * FROM attestations where key_mapped = 'retropgf.round-2.can-vote') v