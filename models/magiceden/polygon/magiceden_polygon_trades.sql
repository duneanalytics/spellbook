{{ config(
        schema = 'magiceden_polygon',
        alias ='trades'
        )
}}

SELECT * FROM {{ ref('magiceden_polygon_events') }}
WHERE evt_type = 'Trade'
