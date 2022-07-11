{{ config(
        alias ='trades'
        )
}}

SELECT * FROM {{ ref('magiceden_solana_transactions') }}
WHERE evt_type = 'Trade'
