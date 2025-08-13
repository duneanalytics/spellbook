{{ safe_table_config(
    blockchain = 'zkevm',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('zkevm', only_official=true, date_filter=true) }}
