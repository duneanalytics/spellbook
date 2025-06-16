{{
    config(
        materialized = 'incremental',
        unique_key = ['blockchain', 'model_name', 'start_time', 'end_time'],
        incremental_strategy = 'merge'
    )
}}

SELECT 
    blockchain,
    model_name,
    start_time,
    end_time,
    'msilb7' as contributor,
    'query' as source,
    timestamp '2023-03-11' as created_at,
    current_timestamp as updated_at
FROM {{ ref('labels_likely_bot_labels') }}
WHERE category = 'run_log'
AND blockchain IS NOT NULL
AND model_name IS NOT NULL
AND start_time IS NOT NULL
AND end_time IS NOT NULL
AND address = 0x