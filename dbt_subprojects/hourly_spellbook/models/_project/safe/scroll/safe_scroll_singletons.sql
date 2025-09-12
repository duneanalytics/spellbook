{{ safe_incremental_singleton_config(
    blockchain = 'scroll',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('scroll', only_official=true) }}
