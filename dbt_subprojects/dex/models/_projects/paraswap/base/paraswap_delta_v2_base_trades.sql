{{ config(
    schema = 'paraswap_delta_v2_base',
    alias = 'trades',

    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.call_block_time')],
    unique_key = ['method', 'call_tx_hash', 'call_trace_address', 'order_index'],
    post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                spell_type = "project",
                                spell_name = "paraswap_delta_v2",
                                contributors = \'["eptighte"]\') }}'
    )
}}

{{ delta_v2_master('base') }}