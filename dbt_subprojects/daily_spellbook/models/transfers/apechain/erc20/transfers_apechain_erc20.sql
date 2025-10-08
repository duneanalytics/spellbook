{{ config(

    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'],
    alias = 'erc20',
    post_hook='{{ expose_spells(\'["apechain"]\',
                                    "sector",
                                    "transfers",
                                    \'["principatel"]\') }}') }}

WITH

erc20_transfers  as (
        SELECT
            'receive' as transfer_type,
            evt_tx_hash,
            evt_index,
            block_time,
            to as wallet_address,
            contract_address as token_address,
            CAST(value as double) as amount_raw
        FROM
        {{ source('erc20_apechain', 'evt_Transfer') }}
        {% if is_incremental() %}
            WHERE {{ incremental_predicate('block_time') }}
        {% endif %}

        UNION ALL

        SELECT
            'send' as transfer_type,
            evt_tx_hash,
            evt_index,
            block_time,
            "from" as wallet_address,
            contract_address as token_address,
            -CAST(value as double) as amount_raw
        FROM
        {{ source('erc20_apechain', 'evt_Transfer') }}
        {% if is_incremental() %}
            WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
)

SELECT
    'apechain' as blockchain,
    transfer_type,
    evt_tx_hash,
    evt_index,
    block_time,
    CAST(date_trunc('month', block_time) as date) as block_month,
    wallet_address,
    token_address,
    amount_raw
FROM
erc20_transfers