{{ safe_incremental_singleton_config(
    blockchain = 'ronin',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('ronin', only_official=true) }}
