{{ safe_incremental_singleton_config(
    blockchain = 'gnosis',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('gnosis', only_official=true) }}
