{{ config(
    schema = 'near',
    alias = 'fact_intents',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['fact_intents_id'],
    partition_by = ['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(\'["near"]\',
                                "project",
                                "defi",
                                \'["krishgka"]\') }}'
) }}

-- NEAR intents tracking nep245 and dip4 standards on intents.near contract

WITH logs_base AS (
    SELECT
        l.block_time,
        l.block_height,
        l.block_date,
        a.tx_hash,
        l.receipt_id,
        l.index_in_execution_outcome_logs AS log_index,
        l.executor_account_id AS receiver_id,
        a.receipt_predecessor_account_id AS predecessor_id,
        a.tx_from AS signer_id,
        a.execution_gas_burnt AS gas_burnt,
        COALESCE(l.event, l.log) AS clean_log,
        TRY(json_parse(COALESCE(l.event, l.log))) AS log_json,
        json_extract_scalar(TRY(json_parse(COALESCE(l.event, l.log))), '$.event') AS log_event,
        json_extract(TRY(json_parse(COALESCE(l.event, l.log))), '$.data') AS log_data,
        json_array_length(json_extract(TRY(json_parse(COALESCE(l.event, l.log))), '$.data')) AS log_data_len,
        l.execution_status NOT LIKE '%FAILURE%' AS receipt_succeeded
    FROM 
        {{ source('near', 'logs') }} l
    LEFT JOIN {{ source('near', 'actions') }} a
        ON l.receipt_id = a.receipt_id
    WHERE 
        l.executor_account_id = 'intents.near'
        AND l.block_date >= DATE '2024-11-01'
        AND json_extract_scalar(TRY(json_parse(COALESCE(l.event, l.log))), '$.standard') IN ('nep245', 'dip4')
        {% if is_incremental() %}
        AND {{ incremental_predicate('l.block_time') }}
        {% endif %}
),

nep245_logs AS (
    SELECT 
        lb.*
    FROM 
        logs_base lb
    WHERE
        json_extract_scalar(lb.log_json, '$.standard') = 'nep245'
),

dip4_logs_raw AS (
    SELECT 
        lb.*,
        json_extract_scalar(
            CAST(json_array_get(lb.log_data, 0) AS JSON),
            '$.referral'
        ) AS referral,
        json_extract_scalar(lb.log_json, '$.version') AS version
    FROM 
        logs_base lb
    WHERE
        json_extract_scalar(lb.log_json, '$.standard') = 'dip4'
),

dip4_logs AS (
    SELECT *
    FROM (
        SELECT 
            *,
            row_number() OVER (
                PARTITION BY receipt_id 
                ORDER BY CASE WHEN referral IS NOT NULL THEN 0 ELSE 1 END
            ) AS rn
        FROM dip4_logs_raw
    )
    WHERE rn = 1
),

flatten_logs AS (
    SELECT
        l.block_time,
        l.block_height,
        l.block_date,
        l.tx_hash,
        l.receipt_id,
        l.receiver_id,
        l.predecessor_id,
        l.log_event,
        l.gas_burnt,
        log_data_element,
        log_data_index,
        TRY(CAST(json_extract(log_data_element, '$.amounts') AS ARRAY(VARCHAR))) AS amounts,
        TRY(CAST(json_extract(log_data_element, '$.token_ids') AS ARRAY(VARCHAR))) AS token_ids,
        json_extract_scalar(log_data_element, '$.owner_id') AS owner_id,
        json_extract_scalar(log_data_element, '$.old_owner_id') AS old_owner_id,
        json_extract_scalar(log_data_element, '$.new_owner_id') AS new_owner_id,
        json_extract_scalar(log_data_element, '$.memo') AS memo,
        l.log_index,
        l.receipt_succeeded,
        cardinality(TRY(CAST(json_extract(log_data_element, '$.amounts') AS ARRAY(VARCHAR)))) AS amounts_size,
        cardinality(TRY(CAST(json_extract(log_data_element, '$.token_ids') AS ARRAY(VARCHAR)))) AS token_ids_size
    FROM
        nep245_logs l
    CROSS JOIN UNNEST(CAST(l.log_data AS ARRAY(JSON))) WITH ORDINALITY AS t(log_data_element, log_data_index)
),

flatten_arrays AS (
    SELECT
        block_time,
        block_height,
        block_date,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        log_event,
        log_index,
        log_data_index - 1 AS log_event_index,
        owner_id,
        old_owner_id,
        new_owner_id,
        memo,
        amount_index - 1 AS amount_index,
        amount_value AS amount_raw,
        element_at(token_ids, amount_index) AS token_id,
        gas_burnt,
        receipt_succeeded
    FROM
        flatten_logs
    CROSS JOIN UNNEST(amounts) WITH ORDINALITY AS t(amount_value, amount_index)
)

SELECT
    final.block_time,
    final.block_height,
    final.block_date,
    final.tx_hash,
    final.receipt_id,
    final.receiver_id,
    final.predecessor_id,
    final.log_event,
    final.log_index,
    final.log_event_index,
    final.owner_id,
    final.old_owner_id,
    final.new_owner_id,
    final.memo,
    final.amount_index,
    final.amount_raw,
    final.token_id,
    dip4.referral,
    dip4.version AS dip4_version,
    final.gas_burnt,
    final.receipt_succeeded,
    {{ dbt_utils.generate_surrogate_key(
        ['final.tx_hash', 'final.receipt_id', 'final.log_index', 'final.log_event_index', 'final.amount_index']
    ) }} AS fact_intents_id
FROM
    flatten_arrays final
LEFT JOIN 
    dip4_logs dip4 
    ON dip4.tx_hash = final.tx_hash
    AND dip4.receipt_id = final.receipt_id

