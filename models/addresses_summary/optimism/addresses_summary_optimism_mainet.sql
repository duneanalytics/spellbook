{{ config(
    tags=['dunesql'],
    alias = alias('mainet'),
    materialized='incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "addresses_summary",
                                \'["Henrystats"]\') }}'
    )
}}

{% if is_incremental() %}
WITH 

weekly_active_addresses as (
    SELECT 
        DISTINCT 
            "from" as address 
        FROM 
        {{ source('optimism', 'transactions') }}
        WHERE block_time >= date_trunc('day', now() - Interval '7' Day)
)

SELECT 
    'optimism' as blockchain, 
    wd.address, 
    fa.block_time as first_active_time, 
    fa.tx_hash as first_transaction_hash, 
    SUM((l1_fee + (gas_used * gas_price))/1e18) as gas_spent,
    MAX(ot.block_time) as last_active_time, 
    CASE
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 1825 THEN '5 years old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 1460 THEN '4 years old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 1095 THEN '3 years old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 730 THEN '2 years old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 365 THEN '1 year old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 91 THEN '3 months old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 30 THEN '1 month old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 7 THEN '1 week old User'
           ELSE 'less than 1 week old User'
    END as address_age, 
    CASE
            WHEN (COUNT(ot.hash)/date_diff('day', min(ot.block_time), max(ot.block_time))) >= 1 THEN 'Daily User'
            WHEN (COUNT(ot.hash)/date_diff('day', min(ot.block_time), max(ot.block_time))) >= 0.142857142857 THEN 'Weekly User'
            WHEN (COUNT(ot.hash)/date_diff('day', min(ot.block_time), max(ot.block_time))) >= 0.0333333333333 THEN 'Monthly User'
            WHEN (COUNT(ot.hash)/date_diff('day', min(ot.block_time), max(ot.block_time))) >= 0.0027397260274 THEN 'Yearly User'
            ELSE 'Sparse User'
    END as usage_frequency,
    COUNT(ot.hash) as number_of_transactions,
    COUNT(DISTINCT(cm.contract_project)) as unique_dapps 

FROM 
weekly_active_addresses wd 
INNER JOIN 
 {{ source('optimism', 'transactions') }} ot 
    ON wd.address = ot."from"
INNER JOIN 
{{ ref('addresses_events_optimism_first_activity') }} fa 
    ON wd.address = fa.address
INNER JOIN 
{{ ref('contracts_optimism_contract_mapping') }} cm 
    ON ot."to" = cm.contract_address 
GROUP BY 1, 2, 3, 4

{% else %}

SELECT 
    'optimism' as blockchain,
    ot."from" as address, 
    fa.block_time as first_active_time, 
    fa.tx_hash as first_transaction_hash, 
    SUM((l1_fee + (gas_used * gas_price))/1e18) as gas_spent,
    MAX(ot.block_time) as last_active_time, 
    CASE
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 1825 THEN '5 years old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 1460 THEN '4 years old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 1095 THEN '3 years old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 730 THEN '2 years old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 365 THEN '1 year old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 91 THEN '3 months old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 30 THEN '1 month old User'
           WHEN date_diff('day', min(ot.block_time), max(ot.block_time)) > 7 THEN '1 week old User'
           ELSE 'less than 1 week old User'
    END as address_age, 
    CASE
            WHEN (date_diff('day', min(ot.block_time), max(ot.block_time))/COUNT(ot.hash)) >= 1 THEN 'Daily User'
            WHEN (date_diff('day', min(ot.block_time), max(ot.block_time))/COUNT(ot.hash)) >= 0.142857142857 THEN 'Weekly User'
            WHEN (date_diff('day', min(ot.block_time), max(ot.block_time))/COUNT(ot.hash)) >= 0.0333333333333 THEN 'Monthly User'
            WHEN (date_diff('day', min(ot.block_time), max(ot.block_time))/COUNT(ot.hash)) >= 0.0027397260274 THEN 'Yearly User'
            ELSE 'Sparse User'
    END as usage_frequency,
    COUNT(ot.hash) as number_of_transactions,
    COUNT(DISTINCT(cm.contract_project)) as unique_dapps 
FROM 
{{ source('optimism', 'transactions') }} ot 
INNER JOIN 
{{ ref('addresses_events_optimism_first_activity') }} fa 
    ON ot."from" = fa.address
INNER JOIN 
{{ ref('contracts_optimism_contract_mapping') }} cm 
    ON ot."to" = cm.contract_address 
GROUP BY 1, 2, 3, 4

{% endif %}
