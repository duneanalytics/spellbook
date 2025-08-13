{{ safe_table_config(
    blockchain = 'unichain',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('unichain', only_official=true, date_filter=true) }}
