{{ config(
    schema = 'addresses_events_optimism'
    
    , alias = 'first_path'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'append'
    , unique_key = ['address']
    )
}}

WITH 

get_addresses as ( -- addresses with at least 3 transactions
    SELECT 
        ot."from" as address, 
        COUNT(*) as num_transactions 
    FROM 
        (
            {% if not is_incremental() %}
            SELECT 
                "from", 
                hash 
            FROM 
            {{ source('optimism', 'transactions') }}

            UNION ALL 

            SELECT 
                "from",
                hash 
            FROM 
            {{ source('optimism_legacy_ovm1', 'transactions') }}
            {% endif %}
            {% if is_incremental() %} -- Only check data from ovm table on first run 
            SELECT 
                "from", 
                hash 
            FROM 
            {{ source('optimism', 'transactions') }} ot 
            LEFT JOIN 
            {{this}} ffb 
                ON ot."from" = ffb.address 
            WHERE ffb.address IS NULL
            {% endif %}
        ) ot 
    GROUP BY 1 
    HAVING COUNT(*) >= 3
),

get_transactions_details as (
        SELECT 
            ot."from" as address, 
            ot.to as to_address,
            ot.value, 
            ot.gas_used,
            ot.block_time,
            ot.block_number,
            bytearray_substring(ot.data, 1, 4) as function 
        FROM 
         (
            {% if not is_incremental() %}
            SELECT 
                "from", 
                to, 
                CAST(value as double) as value, 
                gas_used,
                block_time,
                block_number,
                data
            FROM 
            {{ source('optimism', 'transactions') }}

            UNION ALL 

            SELECT 
                "from", 
                to, 
                CAST(value as double) as value, 
                gas_used,
                block_time,
                block_number,
                data
            FROM 
            {{ source('optimism_legacy_ovm1', 'transactions') }}
            {% endif %}
            {% if is_incremental() %} -- Only check data from ovm table on first run 
            SELECT 
                "from", 
                to, 
                CAST(value as double) as value, 
                gas_used,
                block_time,
                block_number,
                data
            FROM 
            {{ source('optimism', 'transactions') }} ot 
            LEFT JOIN 
            {{this}} ffb 
                ON ot."from" = ffb.address 
            WHERE ffb.address IS NULL 
            {% endif %}
         ) ot 
    INNER JOIN 
    get_addresses ga
        ON ot."from" = ga.address -- make sure address had at least 3 transactions before running row_number
),

get_row_number as (
        SELECT 
            *, 
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_number ASC) as partition_rank
        FROM 
        get_transactions_details
),

filter_for_3 as (
        SELECT 
            * 
        FROM 
        get_row_number
        WHERE partition_rank <= 3 
)

SELECT 
    'optimism' as blockchain,
    f.address, 
    array_agg(f.function ORDER BY partition_rank ASC) as first_functions_path, 
    array_agg(f.to_address ORDER BY partition_rank ASC) as first_to_addresses_path, 
    array_agg(
        (CASE 
            WHEN f.function = 0x AND f.gas_used = 21000 AND f.value > 0 THEN 'eth_transfer'
            ELSE COALESCE(sig.function, CAST(f.function as VARCHAR))
        END)
        ORDER BY partition_rank ASC
    ) as first_functions_name_path,
    array_agg(COALESCE(cm.contract_project, CAST(f.to_address as VARCHAR)) 
    ORDER BY partition_rank ASC) as first_contract_project_path, 
    array_agg(f.block_time ORDER BY partition_rank ASC) as first_block_time_path
FROM 
filter_for_3 f 
LEFT JOIN (
    SELECT 
        DISTINCT id, 
        split_part(signature,'(',1) as function 
    FROM 
    {{ ref('signatures') }} 
    where type = 'function_call'
    AND id NOT IN (0x09779838, 0x00000000) -- for some weird reason these have duplicates functions
) sig 
    ON sig.id = f.function 
LEFT JOIN 
{{ ref('contracts_optimism_contract_mapping') }} cm 
    ON f.to_address = cm.contract_address
GROUP BY 1, 2 
