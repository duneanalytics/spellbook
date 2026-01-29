{{
    config(
        alias = 'tx_hash_labels_early_investment'
        , post_hook='{{ hide_spells() }}'
        
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_early_investment_ethereum') }}