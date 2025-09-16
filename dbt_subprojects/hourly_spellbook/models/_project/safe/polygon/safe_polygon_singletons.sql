{{ safe_incremental_singleton_config(
    blockchain = 'polygon',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('polygon', only_official=true) }}
