{{ config(
    alias = 'all',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address', 'name'],
    partition_by = ['name']
    )
}}

SELECT * FROM 
(
SELECT * FROM {{ ref('static_labels_all') }}
UNION
SELECT * FROM {{ ref('query_labels_all') }}
)

{% if is_incremental() %}
WHERE updated_at >= date_trunc("day", now() - interval '1 week')
{% endif %}