{{
    config(
        alias='trader_frequencies',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\', "sector", "tx_hash_labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('trader_frequencies_ethereum') }}