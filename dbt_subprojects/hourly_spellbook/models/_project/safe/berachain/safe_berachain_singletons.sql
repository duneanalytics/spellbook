{{ safe_incremental_singleton_config(
    blockchain = 'berachain',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('berachain', only_official=true) }}
