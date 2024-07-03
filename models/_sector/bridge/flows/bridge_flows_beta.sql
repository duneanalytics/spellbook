{{ config(
    schema = 'bridge',
    alias = 'flows_beta',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['unique_identifier'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.last_updated')],
    post_hook='{{ expose_spells(blockchains = \'["base"]\',
                                spell_type = "sector",
                                spell_name = "bridge",
                                contributors = \'["hildobby"]\') }}')
}}

{{enrich_bridge_flows(ref('bridge_raw_flows'))}}