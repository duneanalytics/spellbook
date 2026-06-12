{{
  config(
    schema = 'near_v1_signer',
    alias = 'users',
    materialized = 'incremental',
    incremental_strategy = 'append'
    , post_hook='{{ hide_spells() }}'
  )
}}

-- PoC Query: https://dune.com/queries/4642523
WITH sign_requests AS (
  SELECT
    receipt_predecessor_account_id as account_id,
    json_extract_scalar(action_function_call_args_parsed, '$.request.path') AS derivation_path,
    CAST(json_extract_scalar(action_function_call_args_parsed, '$.request.key_version') AS INTEGER) AS key_version
  FROM {{ source('near', 'actions') }} action
  JOIN {{ source('near', 'logs') }} log
  ON action.block_height = log.block_height
  AND action.block_date = log.block_date
  AND action.receipt_id = log.receipt_id
  WHERE
    receipt_receiver_account_id = 'v1.signer'
    -- Deployment block of v1.signer: https://nearblocks.io/txns/79BBi3H3XgzktscGqKDZHAiNGRseuAJuYCmHkxKFLNif
    AND action.block_height >= 124788114
    AND log.block_height >= 124788114
    -- block_date of the deployment block: bounds full refreshes to
    -- post-deployment partitions (block_height alone cannot prune)
    AND action.block_date >= DATE '2024-08-01'
    AND log.block_date >= DATE '2024-08-01'
    {% if is_incremental() %}
    AND {{ incremental_predicate('action.block_date') }}
    AND {{ incremental_predicate('log.block_date') }}
    {% endif %}
    AND action_kind = 'FUNCTION_CALL'
    AND action_function_call_call_method_name = 'sign'
    -- $ echo "eyJyZXF1ZXN0Ijp" | base64 --decode ==> {"request":{
    AND SUBSTRING(
      action_function_call_call_args_base64,
      1,
      LENGTH('eyJyZXF1ZXN0Ijp')
    ) = 'eyJyZXF1ZXN0Ijp'
    AND index_in_execution_outcome_logs = 1
    GROUP BY 1,2,3
)

SELECT
  account_id,
  derivation_path,
  key_version
FROM sign_requests
{% if is_incremental() %}
-- key_version can be NULL (655 of 6136 rows), so a merge unique_key would not
-- dedupe those rows; append only the triples not already in the table
WHERE NOT EXISTS (
  SELECT 1
  FROM {{ this }} existing
  WHERE existing.account_id = sign_requests.account_id
    AND existing.derivation_path IS NOT DISTINCT FROM sign_requests.derivation_path
    AND existing.key_version IS NOT DISTINCT FROM sign_requests.key_version
)
{% endif %}
