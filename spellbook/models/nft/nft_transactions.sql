{{ config(
        alias ='transactions')
}}

SELECT * FROM {{ ref('opensea_transactions') }} 
         UNION
SELECT * FROM {{ ref('magiceden_transactions') }}