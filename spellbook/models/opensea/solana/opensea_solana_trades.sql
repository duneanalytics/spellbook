{{ config(
        alias ='trades'
        )
}}

SELECT * FROM {{ ref('opensea_solana_transactions') }}
WHERE evt_type = 'Trade'
