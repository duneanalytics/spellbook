{{
    config(
        alias = 'tx_hash_labels_harvest_yield',
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "tx_hash_labels", \'["gentrexha"]\') }}',
        
        
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_harvest_yield_ethereum') }}