{{ safe_incremental_singleton_config(
    blockchain = 'zksync',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('zksync', only_official=true) }}
