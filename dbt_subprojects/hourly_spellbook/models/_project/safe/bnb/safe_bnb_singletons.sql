{{ safe_incremental_singleton_config(
    blockchain = 'bnb',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('bnb', only_official=true) }}
