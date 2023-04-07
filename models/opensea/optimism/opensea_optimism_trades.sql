{{ config(
        alias ='trades'
        )
}}

SELECT * FROM {{ ref('opensea_optimism_events') }}
WHERE evt_type = 'Trade'
