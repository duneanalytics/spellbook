{{ config(
        alias ='trades'
        )
}}

SELECT * FROM {{ ref('magiceden_solana_events') }}
WHERE evt_type = 'Trade'
