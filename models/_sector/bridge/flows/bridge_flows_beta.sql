{{ config(
    schema = 'bridge',
    alias = 'flows_beta',
    partition_by = ['blockchain','project','block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain','tx_hash','evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                spell_type = "sector",
                                spell_name = "bridge",
                                contributors = \'["hildobby"]\') }}')
}}

{{enrich_bridge_flows(ref('bridge_raw_flows'))}}