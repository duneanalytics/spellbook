{{config(
    alias = 'validators'
    , post_hook='{{ hide_spells() }}'
    )
}}

SELECT * FROM  {{ ref('labels_validators_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_validators_bnb') }}
UNION
SELECT * FROM  {{ ref('labels_validators_solana') }}