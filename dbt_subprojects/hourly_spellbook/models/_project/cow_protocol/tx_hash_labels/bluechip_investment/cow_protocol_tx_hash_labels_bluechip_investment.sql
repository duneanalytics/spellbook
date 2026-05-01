{{
    config(
        alias = 'tx_hash_labels_bluechip_investment'
        , post_hook='{{ hide_spells() }}'
        
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_bluechip_investment_ethereum') }}
