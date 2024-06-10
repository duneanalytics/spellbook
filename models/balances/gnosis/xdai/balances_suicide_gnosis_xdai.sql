{{ config(
    
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'trace_address', 'address', 'block_time'], 
    alias = 'xdai_suicide',
    post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "transfers",
                                    \'["hdser"]\') }}') }}

WITH 

suicide AS (
    SELECT 
        block_time
        ,block_number
        ,CAST(date_trunc('month', block_time) as date) as block_month
        ,tx_hash
        ,tx_index
        ,trace_address
        ,address
        ,refund_address
    FROM 
        {{ source('gnosis', 'traces') }}
    WHERE
        type = 'suicide'
        AND
        success
        {% if is_incremental() %}
        AND block_time >= date_trunc('day', now() - interval '3' Day)
        {% endif %}
)

SELECT * FROM suicide