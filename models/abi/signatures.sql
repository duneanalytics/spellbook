{{ config(
        alias = 'signatures',
        partition_by = ['created_at'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['created_at', 'unique_signature_id'],
        )
}}

{% set chains = ['ethereum', 'optimism', 'arbitrum', 'avalanche_c', 'polygon', 'bnb', 'gnosis'] %}

WITH
    signatures as (
        {% for chain in chains %}
            SELECT
                *
                , concat(id, signature, type) as unique_signature_id
            FROM {{ source('{{chain}}','signatures') }}
            
            {% if is_incremental() %}
            WHERE created_at >= date_trunc("day", now() - interval '2 days')
            {% endif %}

            {% if not loop.last %}
            union
            {% endif %}

        {% endfor %}
    )

   SELECT * FROM signatures