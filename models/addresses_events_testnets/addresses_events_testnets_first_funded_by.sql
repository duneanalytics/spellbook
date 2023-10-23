{{ config
(
    alias = 'first_funded_by'
    
    , post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "addresses_events_testnets",
                                    \'["hildobby"]\') }}'
)
}}

SELECT *
FROM (
    SELECT blockchain
    , address
    , first_funded_by
    , block_time
    , block_number
    , tx_hash
    FROM {{ ref('addresses_events_testnets_goerli_first_funded_by') }}
)
