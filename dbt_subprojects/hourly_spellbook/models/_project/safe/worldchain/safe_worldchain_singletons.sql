{{ safe_incremental_singleton_config(
    blockchain = 'worldchain',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('worldchain', only_official=true) }}
