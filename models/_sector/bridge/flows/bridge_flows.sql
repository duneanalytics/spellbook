{{ config(
    schema = 'bridge',
    alias = 'flows',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{{enrich_bridge_flows(ref('bridge_raw_flows'))}}