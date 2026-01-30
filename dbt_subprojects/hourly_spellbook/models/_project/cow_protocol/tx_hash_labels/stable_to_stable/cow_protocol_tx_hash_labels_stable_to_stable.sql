{{
    config(
        alias = 'tx_hash_labels_stable_to_stable'
        , post_hook='{{ hide_spells() }}'
        
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_stable_to_stable_ethereum') }}