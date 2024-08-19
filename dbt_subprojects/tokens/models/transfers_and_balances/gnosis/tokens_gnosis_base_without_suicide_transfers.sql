{{config(
    schema = 'tokens_gnosis',
    alias = 'base_wihout_suicide_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
)
}}

WITH 

tokens_gnosis_base_transfers AS (
    SELECT 
        unique_key
        , blockchain
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , trace_address
        , token_standard
        , tx_from
        , tx_to
        , tx_index
        , "from"
        , to
        , contract_address
        , amount_raw
    FROM 
        {{ ref('tokens_gnosis_base_transfers') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
),

tokens_gnosis_base_non_standard_transfers AS (
    SELECT 
        unique_key
        , blockchain
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , trace_address
        , token_standard
        , tx_from
        , tx_to
        , tx_index
        , "from"
        , to
        , contract_address
        , amount_raw
    FROM 
        {{ ref('tokens_gnosis_base_non_standard_transfers') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
)

SELECT * FROM tokens_gnosis_base_transfers
UNION ALL 
SELECT * FROM tokens_gnosis_base_non_standard_transfers