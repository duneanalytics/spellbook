{{ safe_incremental_singleton_config(
    blockchain = 'zkevm',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('zkevm', only_official=true) }}
