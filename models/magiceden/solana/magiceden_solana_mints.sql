{{ config(
        alias ='mints'

        )
}}

SELECT * FROM {{ ref('magiceden_solana_events') }}
WHERE evt_type = 'Mint'
