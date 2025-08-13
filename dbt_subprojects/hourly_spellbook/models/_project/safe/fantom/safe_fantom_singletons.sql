{{ safe_table_config(
    blockchain = 'fantom',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('fantom', only_official=true, date_filter=true) }}
