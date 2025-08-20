-- ref dbt_subprojects/dex/models/_projects/paraswap/ethereum/paraswap_v6_ethereum_trades.sql
-- ref dbt_subprojects/dex/models/_projects/paraswap/ethereum/paraswap_v6_ethereum_trades_decoded.sql
{{ config(
    schema = 'paraswap_delta_v1_ethereum',
    alias = 'trades',

    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['method', 'tx_hash', 'trace_address', 'order_index'],
    post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                spell_type = "project",
                                spell_name = "paraswap_delta_v1",
                                contributors = \'["eptighte"]\') }}'
    )
}}

{% set project_start_date = '2024-05-01' %}

{{ delta_v1_master('ethereum') }}