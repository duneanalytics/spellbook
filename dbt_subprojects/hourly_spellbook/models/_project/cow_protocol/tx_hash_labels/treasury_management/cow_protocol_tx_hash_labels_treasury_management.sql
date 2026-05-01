{{
    config(
        alias = 'tx_hash_labels_treasury_management'
        , post_hook='{{ hide_spells() }}'
        
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_treasury_management_ethereum') }}