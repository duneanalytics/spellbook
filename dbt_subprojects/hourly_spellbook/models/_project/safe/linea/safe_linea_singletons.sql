{{ safe_table_config(
    blockchain = 'linea',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('linea', only_official=true, date_filter=true) }}
