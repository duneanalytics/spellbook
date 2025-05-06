{{
  config(
    schema = 'near_v1_signer',
    alias = 'users',
    materialized = 'table',
    post_hook = '{{ expose_spells(\'["near"]\',
                                    "project",
                                    "v1_signer",
                                    \'["bh2smith"]\') }}'
  )
}}

-- PoC Query: https://dune.com/queries/4642523
SELECT
  receipt_predecessor_account_id as account_id,
  json_extract_scalar(action_function_call_args_parsed, '$.request.path') AS derivation_path,
  CAST(json_extract_scalar(action_function_call_args_parsed, '$.request.key_version') AS INTEGER) AS key_version
FROM {{ source('near', 'actions') }} action
JOIN {{ source('near', 'logs') }} log
ON action.block_height = log.block_height
AND action.receipt_id = log.receipt_id
WHERE
  receipt_receiver_account_id = 'v1.signer'
  -- Deployment block of v1.signer: https://nearblocks.io/txns/79BBi3H3XgzktscGqKDZHAiNGRseuAJuYCmHkxKFLNif
  AND action.block_height >= 124788114
  AND log.block_height >= 124788114
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
