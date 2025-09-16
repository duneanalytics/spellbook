{{ safe_incremental_singleton_config(
    blockchain = 'base',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('base', only_official=true) }}
