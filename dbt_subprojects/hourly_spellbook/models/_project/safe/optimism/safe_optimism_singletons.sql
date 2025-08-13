{{ safe_table_config(
    blockchain = 'optimism',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('optimism', only_official=true, date_filter=true) }}
