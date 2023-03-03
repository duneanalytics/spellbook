{{ config(
        alias ='trades'
        )
}}

SELECT * FROM {{ ref('opensea_solana_events') }}
WHERE evt_type = 'Trade'
