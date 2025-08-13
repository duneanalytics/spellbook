{{ safe_table_config(
    blockchain = 'mantle',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('mantle', only_official=true, date_filter=true) }}
