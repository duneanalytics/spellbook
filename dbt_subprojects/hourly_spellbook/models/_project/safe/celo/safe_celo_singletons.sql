{{ safe_incremental_singleton_config(
    blockchain = 'celo',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('celo', only_official=true) }}
