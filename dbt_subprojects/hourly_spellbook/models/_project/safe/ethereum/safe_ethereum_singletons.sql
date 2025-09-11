{{ safe_incremental_singleton_config(
    blockchain = 'ethereum',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('ethereum', only_official=true) }}
