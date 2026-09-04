{{ config(
    schema = 'monitoring'
    , alias = 'config_spellbook_solana'
    , materialized = 'table'
    , file_format = 'delta'
) }}

/*
    The solana subproject's arm of the freshness monitoring config: one row per model
    here declaring `meta.monitoring.enabled`, read from the parsed manifest. One writer table
    per subproject — subprojects deploy independently and Delta takes no concurrent writers.
    Consumers read the delta_prod.monitoring.config view in curated-data, never this table.
    No ref() on purpose: it reads declarations, not data, so it emits rows even when upstream
    builds fail.
*/

{{ monitoring_config_select() }}
