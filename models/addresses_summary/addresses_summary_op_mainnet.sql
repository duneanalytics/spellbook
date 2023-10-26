{{ config(
    
    alias = 'op_mainnet',
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
            COUNT(*) as num_txns,
            "from" as address
        FROM
        {{ source('optimism', 'transactions') }}
        WHERE block_time >= date_trunc('day', now() - Interval '7' Day)
        GROUP BY 2 -- optimize with group by
)

SELECT
    'optimism' as blockchain,
    wd.address,
    fa.first_block_time as first_active_time,
    fa.first_tx_hash as first_tx_hash,
    fa.first_function,
    SUM(gas_spent) as gas_spent,
    MAX(ot.block_time) as last_active_time,
    CASE
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 1825 THEN '5 years old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 1460 THEN '4 years old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 1095 THEN '3 years old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 730 THEN '2 years old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 365 THEN '1 year old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 91 THEN '3 months old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 30 THEN '1 month old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 7 THEN '1 week old User'
           ELSE 'less than 1 week old User'
    END as address_age,
    CASE
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 1825 THEN 'Last active more than 5 Years Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 1460 THEN 'Last active more than 4 Years Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 1095 THEN 'Last active more than 3 Years Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 730 THEN 'Last active more than 2 Years Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 365 THEN 'Last active more than 1 Year Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 91 THEN 'Last active more than 3 months Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 30 THEN 'Last active more than 1 month Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 7 THEN 'Last active more than 1 week Ago'
           ELSE 'Active within the past week'
    END as recency_age,
    date_diff('day', min(fa.first_block_time), max(ot.block_time)) as address_age_in_days,
    date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) as recency_in_days,
    CASE
        WHEN (date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp))) = 0 THEN 'First Time User'
        WHEN (date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp))) != 0 AND COUNT(ot.hash) = 1 THEN 'One Time User'
        WHEN (date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp))) != 0 AND COUNT(ot.hash) != 1 THEN (
            CASE 
                WHEN COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) >= 1 THEN 'Daily User'
                WHEN COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) >= 0.14285714 AND COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) < 1 THEN 'Weekly User'
                WHEN COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) >= 0.03333333 AND COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) < 0.14285714 THEN 'Monthly User'
                WHEN COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) >= 0.00273973 AND COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) < 0.03333333 THEN 'Yearly User'
                ELSE 'Sparse Trader'
            END 
        )
    END as usage_frequency,
    COUNT(ot.hash) as number_of_transactions,
    COUNT(DISTINCT(cm.contract_project)) as unique_dapps,
    MIN_BY(cm.contract_project, ot.block_number) as first_to_project,
    MIN_BY(ot.to, ot.block_number) as first_to_address,
    MAX_BY(cm.contract_project, ot.block_number) as last_to_project, 
    MAX_BY(ot.to, ot.block_number) as last_to_address,
    bf.token_amount as total_bridged_eth
FROM 
weekly_active_addresses wd 
INNER JOIN (
        SELECT
            "from",
            to,
            hash,
            block_time,
            case when gas_price = cast(0 as uint256) then 0
            else cast(gas_used as double) * cast(gas_price as double)/1e18 + cast(l1_fee as double) /1e18
            end as gas_spent,
            block_number
        FROM
        {{ source('optimism', 'transactions') }}

        UNION ALL

        SELECT
            "from",
            to,
            hash,
            block_time,
            case when gas_price = 0 then 0
            else cast(gas_limit as double) * cast(gas_price as double)/1e18
            end as gas_spent,
            block_number
        FROM
        {{ source('optimism_legacy_ovm1', 'transactions') }}
) ot
    ON wd.address = ot."from"
INNER JOIN
{{ ref('addresses_events_optimism_first_activity') }} fa
    ON wd.address = fa.address
LEFT JOIN 
{{ ref('contracts_optimism_contract_mapping') }} cm 
    ON ot."to" = cm.contract_address  
LEFT JOIN (
    SELECT 
        SUM(token_amount) as token_amount,
        receiver
    FROM 
    {{ ref('optimism_standard_bridge_flows') }} bf 
    WHERE destination_chain_name = 'Optimism'
    AND token_symbol = 'ETH'
    GROUP BY 2 
) bf
    ON wd.address = bf.receiver
GROUP BY 1, 2, 3, 4, 5, 19 

{% else %}

SELECT
    'optimism' as blockchain,
    ot."from" as address,
    fa.first_block_time as first_active_time,
    fa.first_tx_hash as first_tx_hash,
    fa.first_function,
    SUM(gas_spent) as gas_spent,
    MAX(ot.block_time) as last_active_time,
    CASE
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 1825 THEN '5 years old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 1460 THEN '4 years old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 1095 THEN '3 years old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 730 THEN '2 years old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 365 THEN '1 year old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 91 THEN '3 months old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 30 THEN '1 month old User'
           WHEN date_diff('day', min(fa.first_block_time), max(ot.block_time)) > 7 THEN '1 week old User'
           ELSE 'less than 1 week old User'
    END as address_age,
    CASE
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 1825 THEN 'Last active more than 5 Years Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 1460 THEN 'Last active more than 4 Years Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 1095 THEN 'Last active more than 3 Years Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 730 THEN 'Last active more than 2 Years Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 365 THEN 'Last active more than 1 Year Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 91 THEN 'Last active more than 3 months Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 30 THEN 'Last active more than 1 month Ago'
           WHEN date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) > 7 THEN 'Last active more than 1 week Ago'
           ELSE 'Active within the past week'
    END as recency_age,
    date_diff('day', min(fa.first_block_time), max(ot.block_time)) as address_age_in_days,
    date_diff('day', max(ot.block_time), CAST(NOW() as timestamp)) as recency_in_days,
    CASE
        WHEN (date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp))) = 0 THEN 'First Time User'
        WHEN (date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp))) != 0 AND COUNT(ot.hash) = 1 THEN 'One Time User'
        WHEN (date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp))) != 0 AND COUNT(ot.hash) != 1 THEN (
            CASE 
                WHEN COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) >= 1 THEN 'Daily User'
                WHEN COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) >= 0.14285714 AND COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) < 1 THEN 'Weekly User'
                WHEN COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) >= 0.03333333 AND COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) < 0.14285714 THEN 'Monthly User'
                WHEN COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) >= 0.00273973 AND COUNT(ot.hash)/CAST(date_diff('day', min(fa.first_block_time), CAST(NOW() as timestamp)) as double) < 0.03333333 THEN 'Yearly User'
                ELSE 'Sparse Trader'
            END 
        )
    END as usage_frequency,
    COUNT(ot.hash) as number_of_transactions,
    COUNT(DISTINCT(cm.contract_project)) as unique_dapps,
    MIN_BY(cm.contract_project, ot.block_number) as first_to_project,
    MIN_BY(ot.to, ot.block_number) as first_to_address,
    MAX_BY(cm.contract_project, ot.block_number) as last_to_project, 
    MAX_BY(ot.to, ot.block_number) as last_to_address,
    bf.token_amount as total_bridged_eth
FROM 
(
        SELECT
            "from",
            to,
            hash,
            block_time,
            case when gas_price = cast(0 as uint256) then 0
            else cast(gas_used as double) * cast(gas_price as double)/1e18 + cast(l1_fee as double) /1e18
            end as gas_spent,
            block_number
        FROM
        {{ source('optimism', 'transactions') }}

        UNION ALL

        SELECT
            "from",
            to,
            hash,
            block_time,
            case when gas_price = 0 then 0
            else cast(gas_limit as double) * cast(gas_price as double)/1e18
            end as gas_spent,
            block_number
        FROM
        {{ source('optimism_legacy_ovm1', 'transactions') }}
) ot
INNER JOIN
{{ ref('addresses_events_optimism_first_activity') }} fa
    ON ot."from" = fa.address
LEFT JOIN 
{{ ref('contracts_optimism_contract_mapping') }} cm 
    ON ot."to" = cm.contract_address
LEFT JOIN (
    SELECT 
        SUM(token_amount) as token_amount,
        receiver
    FROM 
    {{ ref('optimism_standard_bridge_flows') }} bf 
    WHERE destination_chain_name = 'Optimism'
    AND token_symbol = 'ETH'
    GROUP BY 2 
) bf
    ON ot."from" = bf.receiver
GROUP BY 1, 2, 3, 4, 5, 19 

{% endif %}
