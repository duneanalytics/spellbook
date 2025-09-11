{{ safe_incremental_singleton_config(
    blockchain = 'optimism',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('optimism', only_official=true) }}
