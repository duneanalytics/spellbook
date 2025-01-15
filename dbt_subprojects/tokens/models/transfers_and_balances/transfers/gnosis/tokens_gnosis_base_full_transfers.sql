{{config(
    schema = 'tokens_gnosis',
    alias = 'base_full_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
)
}}

WITH 

tokens_gnosis_base_without_suicide_transfers AS (
    SELECT 
        *
    FROM 
        {{ ref('tokens_gnosis_base_without_suicide_transfers') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
),

tokens_gnosis_suicide_transfers AS (
    SELECT 
        *
    FROM 
        {{ ref('tokens_gnosis_suicide_transfers') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
)

SELECT * FROM tokens_gnosis_base_without_suicide_transfers
UNION ALL 
SELECT * FROM tokens_gnosis_suicide_transfers