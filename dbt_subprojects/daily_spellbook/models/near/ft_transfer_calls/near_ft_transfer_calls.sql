{{
  config(
    schema = 'near_ft_transfer_calls',
    alias = 'ft_transfer_calls',
    materialized = 'table',
    post_hook = '{{ expose_spells(\'["near"]\',
                                    "project",
                                    "ft_transfer_calls",
                                    \'["danzipie"]\') }}'
  )
}}

WITH
    ft_transfer_calls AS (
        SELECT
            block_date,
            block_height,
            block_time,
            block_hash,
            chunk_hash,
            shard_id,
            tx_hash,
            action_function_call_args_parsed,
            receipt_id
        FROM
            {{ source('near', 'actions') }}
        WHERE
            action_function_call_call_method_name = 'ft_transfer_call'
    )
SELECT
    ft_transfer_calls.block_date,
    ft_transfer_calls.block_height,
    ft_transfer_calls.block_time,
    ft_transfer_calls.block_hash,
    ft_transfer_calls.chunk_hash,
    ft_transfer_calls.shard_id,
    'nep141' AS standard,
    ft_resolve_transfers.receipt_receiver_account_id AS token_id,
    ft_transfer_calls.receipt_id,
    ft_resolve_transfers.receipt_id as resolve_receipt_id,
    ft_resolve_transfers.tx_status AS resolve_status,
    ft_resolve_transfers.tx_hash,
    'ft_transfer_call' AS cause,
    CAST(
        json_extract(ft_transfer_calls.action_function_call_args_parsed, '$.memo') AS varchar
    ) AS memo,
    CAST(
        json_extract(
            ft_resolve_transfers.action_function_call_args_parsed,
            '$.receiver_id'
        ) AS varchar
    ) AS affected_account_id,
    CAST(
        json_extract(
            ft_resolve_transfers.action_function_call_args_parsed,
            '$.sender_id'
        ) AS varchar
    ) AS involved_account_id,
    CAST(
        json_extract(
            ft_resolve_transfers.action_function_call_args_parsed,
            '$.amount'
        ) AS varchar
    ) AS delta_amount,
    ft_resolve_transfers._updated_at,
    ft_resolve_transfers._ingested_at
FROM
    {{ source('near', 'actions') }} AS ft_resolve_transfers
    JOIN ft_transfer_calls ON ft_resolve_transfers.tx_hash = ft_transfer_calls.tx_hash
WHERE
    ft_resolve_transfers.receipt_predecessor_account_id = ft_resolve_transfers.receipt_receiver_account_id
    AND ft_resolve_transfers.action_function_call_call_method_name = 'ft_resolve_transfer'
    AND json_extract(
        ft_resolve_transfers.action_function_call_args_parsed,
        '$.amount'
    ) IS NOT NULL -- this excludes the token_id 'aurora' that is not fully nep141 compliant