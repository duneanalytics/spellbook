{{
    config(
        alias = alias('tx_hash_labels_treasury_management'),
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "tx_hash_labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('cow_protocol_tx_hash_labels_treasury_management_ethereum') }}