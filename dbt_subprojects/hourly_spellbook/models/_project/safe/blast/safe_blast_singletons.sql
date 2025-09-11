{{ safe_incremental_singleton_config(
    blockchain = 'blast',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('blast', only_official=true) }}
