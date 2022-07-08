{{ config(
        alias ='mints'
        )
}}

SELECT * FROM {{ ref('magiceden_solana_transactions') }}
WHERE evt_type = 'Mint'
