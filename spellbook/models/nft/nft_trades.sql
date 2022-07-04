{{ config(
        alias ='trades'
        )
}}

SELECT * FROM {{ ref('opensea_trades') }} 
         UNION
SELECT * FROM {{ ref('magiceden_trades') }}