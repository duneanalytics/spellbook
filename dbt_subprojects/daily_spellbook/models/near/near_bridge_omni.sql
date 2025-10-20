{{ config(
    schema = 'near',
    alias = 'bridge_omni',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['bridge_omni_id'],
    partition_by = ['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(\'["near"]\',
                                "project",
                                "bridge",
                                \'["krishgka"]\') }}'
)}}

-- NEAR Omni Bridge transactions tracking inbound and outbound transfers
-- Monitors omni.bridge.near and related helper contracts

WITH near_omni_contracts AS (
    SELECT contract_address
    FROM (
        VALUES
            ('omni.bridge.near'), -- Main Omni Bridge contract
            ('omni-provider.bridge.near'), -- Helper contract
            ('vaa-prover.bridge.near'), -- Wormhole verification helper contract
            ('omni-relayer.bridge.near') -- Omni Relayer contract
    ) AS contracts(contract_address)
),

actions AS (
    SELECT
        block_height,
        block_time,
        block_date,
        tx_hash,
        tx_status,
        tx_to,
        tx_from,
        receipt_predecessor_account_id,
        receipt_receiver_account_id,
        execution_status,
        index_in_action_receipt AS action_index,
        action_kind AS action_name,
        CAST(json_parse(action_function_call_args_parsed) AS VARCHAR) AS action_data,
        action_function_call_call_method_name AS method_name,
        action_function_call_call_args_base64 AS args,
        receipt_id,
        floor(block_height / 1000) * 1000 AS _partition_by_block_number
    FROM
        {{ source('near', 'actions') }}
    WHERE 
        action_kind = 'FUNCTION_CALL'
        AND (
            tx_to IN (SELECT contract_address FROM near_omni_contracts) 
            OR receipt_receiver_account_id IN (SELECT contract_address FROM near_omni_contracts)
            OR receipt_predecessor_account_id IN (SELECT contract_address FROM near_omni_contracts)
        )
        AND block_date >= DATE '2025-01-01'
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
),

has_mint_burn AS (
    SELECT 
        tx_hash,
        MAX(CASE WHEN method_name = 'mint' THEN TRUE ELSE FALSE END) AS has_mint,
        MAX(CASE WHEN method_name = 'burn' THEN TRUE ELSE FALSE END) AS has_burn
    FROM 
        actions
    GROUP BY 
        1
),

logs AS (
    SELECT
        block_height,
        block_time,
        block_date,
        tx_hash,
        executor_account_id AS receiver_id,
        executor_account_id AS predecessor_id,
        executor_account_id AS signer_id,
        execution_gas_burnt AS gas_burnt,
        COALESCE(event, log) AS clean_log,
        execution_status = 'SUCCESS_VALUE' AS receipt_succeeded,
        receipt_id,
        CAST(block_height AS VARCHAR) || '-' || CAST(receipt_id AS VARCHAR) || '-' || CAST(index_in_execution_outcome_logs AS VARCHAR) AS logs_id,
        index_in_execution_outcome_logs AS log_index
    FROM
        {{ source('near', 'logs') }}
    WHERE
        block_date >= DATE '2025-01-01'
        AND executor_account_id IN (SELECT contract_address FROM near_omni_contracts)
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
),

joined AS (
    SELECT
        a.block_height, 
        a.block_time,
        a.block_date,
        a.tx_hash,
        a.tx_status = 'SUCCESS_VALUE' AS tx_succeeded,
        a.tx_to AS tx_receiver,
        a.tx_from AS tx_signer,
        a.receipt_predecessor_account_id,
        a.receipt_receiver_account_id,
        a.execution_status = 'SUCCESS_VALUE' AS receipt_succeeded,
        a.action_index,
        a.action_name,
        a.action_data,
        a.method_name,
        l.log_index,
        l.clean_log,
        a.receipt_id,
        l.logs_id,
        COALESCE(mb.has_mint, FALSE) AS has_mint,
        COALESCE(mb.has_burn, FALSE) AS has_burn,
        a._partition_by_block_number
    FROM
        actions a
    JOIN logs l
        ON a.block_height = l.block_height 
        AND a.tx_hash = l.tx_hash 
        AND a.receipt_id = l.receipt_id
    LEFT JOIN has_mint_burn mb
        ON a.tx_hash = mb.tx_hash
),

inbound_omni AS (
    -- Inbound transfers: fin_transfer with FinTransferEvent and mint method calls
    SELECT
        block_height, 
        block_time,
        block_date,
        tx_hash,
        tx_receiver,
        tx_signer,
        receipt_predecessor_account_id,
        receipt_receiver_account_id AS bridge_address,
        action_index,
        action_data,
        method_name,
        receipt_id,
        clean_log,
        receipt_succeeded,
        has_mint,
        FALSE AS has_burn,
        TRY(CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON))) AS event_json,
        TRY(json_extract(
            CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
            '$.FinTransferEvent.transfer_message'
        )) AS transfer_data,
        TRY(CAST(json_extract_scalar(
            json_extract(
                CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                '$.FinTransferEvent.transfer_message'
            ),
            '$.amount'
        ) AS BIGINT)) AS amount_raw,
        split_part(
            json_extract_scalar(
                json_extract(
                    CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                    '$.FinTransferEvent.transfer_message'
                ),
                '$.recipient'
            ), ':', 1
        ) AS destination_chain,
        split_part(
            json_extract_scalar(
                json_extract(
                    CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                    '$.FinTransferEvent.transfer_message'
                ),
                '$.recipient'
            ), ':', 2
        ) AS destination_address,
        split_part(
            json_extract_scalar(
                json_extract(
                    CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                    '$.FinTransferEvent.transfer_message'
                ),
                '$.sender'
            ), ':', 1
        ) AS source_chain,
        split_part(
            json_extract_scalar(
                json_extract(
                    CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                    '$.FinTransferEvent.transfer_message'
                ),
                '$.sender'
            ), ':', 2
        ) AS source_address,
        split_part(
            json_extract_scalar(
                json_extract(
                    CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                    '$.FinTransferEvent.transfer_message'
                ),
                '$.token'
            ), ':', 2
        ) AS token_address,
        json_extract_scalar(
            json_extract(
                CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                '$.FinTransferEvent.transfer_message'
            ),
            '$.token'
        ) AS raw_token_id,
        TRY(CAST(json_extract_scalar(
            json_extract(
                CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                '$.FinTransferEvent.transfer_message'
            ),
            '$.origin_nonce'
        ) AS BIGINT)) AS origin_nonce,
        TRY(CAST(json_extract_scalar(
            json_extract(
                CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                '$.FinTransferEvent.transfer_message'
            ),
            '$.destination_nonce'
        ) AS BIGINT)) AS destination_nonce,
        json_extract_scalar(
            json_extract(
                CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                '$.FinTransferEvent.transfer_message'
            ),
            '$.msg'
        ) AS memo,
        'inbound' AS direction,
        _partition_by_block_number
    FROM
        joined
    WHERE
        clean_log LIKE '%FinTransferEvent%'
),

outbound_omni AS (
    -- Outbound transfers: ft_on_transfer, burn, and ft_resolve_transfer with InitTransferEvent
    SELECT
        block_height, 
        block_time,
        block_date,
        tx_hash,
        tx_receiver,
        tx_signer,
        receipt_predecessor_account_id,
        receipt_receiver_account_id AS bridge_address,
        action_index,
        action_data,
        method_name,
        receipt_id,
        clean_log,
        receipt_succeeded,
        FALSE AS has_mint,
        has_burn,
        TRY(CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON))) AS event_json,
        TRY(json_extract(
            CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
            '$.InitTransferEvent.transfer_message'
        )) AS transfer_data,
        TRY(CAST(json_extract_scalar(
            json_extract(
                CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                '$.InitTransferEvent.transfer_message'
            ),
            '$.amount'
        ) AS BIGINT)) AS amount_raw,
        split_part(
            json_extract_scalar(
                json_extract(
                    CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                    '$.InitTransferEvent.transfer_message'
                ),
                '$.recipient'
            ), ':', 1
        ) AS destination_chain,
        split_part(
            json_extract_scalar(
                json_extract(
                    CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                    '$.InitTransferEvent.transfer_message'
                ),
                '$.recipient'
            ), ':', 2
        ) AS destination_address,
        split_part(
            json_extract_scalar(
                json_extract(
                    CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                    '$.InitTransferEvent.transfer_message'
                ),
                '$.sender'
            ), ':', 1
        ) AS source_chain,
        split_part(
            json_extract_scalar(
                json_extract(
                    CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                    '$.InitTransferEvent.transfer_message'
                ),
                '$.sender'
            ), ':', 2
        ) AS source_address,
        split_part(
            json_extract_scalar(
                json_extract(
                    CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                    '$.InitTransferEvent.transfer_message'
                ),
                '$.token'
            ), ':', 2
        ) AS token_address,
        json_extract_scalar(
            json_extract(
                CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                '$.InitTransferEvent.transfer_message'
            ),
            '$.token'
        ) AS raw_token_id,
        TRY(CAST(json_extract_scalar(
            json_extract(
                CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                '$.InitTransferEvent.transfer_message'
            ),
            '$.origin_nonce'
        ) AS BIGINT)) AS origin_nonce,
        TRY(CAST(json_extract_scalar(
            json_extract(
                CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                '$.InitTransferEvent.transfer_message'
            ),
            '$.destination_nonce'
        ) AS BIGINT)) AS destination_nonce,
        json_extract_scalar(
            json_extract(
                CAST(json_parse(regexp_extract(clean_log, '\{.*\}')) AS MAP(VARCHAR, JSON)),
                '$.InitTransferEvent.transfer_message'
            ),
            '$.msg'
        ) AS memo,
        'outbound' AS direction,
        _partition_by_block_number
    FROM
        joined
    WHERE
        clean_log LIKE '%InitTransferEvent%'
), 

final AS (
    SELECT
        block_height,
        block_time,
        block_date,
        tx_hash,
        token_address,
        amount_raw,
        memo,
        destination_address,
        source_address,
        destination_chain AS destination_chain_id,
        source_chain AS source_chain_id,
        raw_token_id,
        direction,
        receipt_succeeded,
        method_name,
        bridge_address,
        has_mint,
        has_burn,
        _partition_by_block_number
    FROM
        inbound_omni
    UNION ALL
    SELECT
        block_height,
        block_time,
        block_date,
        tx_hash,
        token_address,
        amount_raw,
        memo,
        destination_address,
        source_address,
        destination_chain AS destination_chain_id,
        source_chain AS source_chain_id,
        raw_token_id,
        direction,
        receipt_succeeded,
        method_name,
        bridge_address,
        has_mint,
        has_burn,
        _partition_by_block_number
    FROM
        outbound_omni
) 

SELECT
    block_height,
    block_time,
    block_date,
    tx_hash,
    token_address,
    amount_raw,
    amount_raw AS amount_adj,
    memo,
    destination_address,
    source_address,
    destination_chain_id,
    source_chain_id,
    raw_token_id,
    direction,
    receipt_succeeded,
    method_name,
    bridge_address,
    has_mint,
    has_burn,
    'omni' AS platform,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'source_chain_id', 'destination_address', 'token_address', 'amount_raw']
    ) }} AS bridge_omni_id,
    _partition_by_block_number
FROM 
    final

