{{ config(
    schema = 'paraswap_delta_v2_optimism',
    alias = 'trades',

    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['method', 'tx_hash', 'trace_address', 'order_index'],
    post_hook='{{ expose_spells(blockchains = \'["optimism"]\',
                                spell_type = "project",
                                spell_name = "paraswap_delta_v2",
                                contributors = \'["eptighte"]\') }}'
    )
}}

{{ delta_v2_master('optimism') }}