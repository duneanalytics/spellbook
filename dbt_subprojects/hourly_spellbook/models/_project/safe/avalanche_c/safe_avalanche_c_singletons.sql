{{ safe_table_config(
    blockchain = 'avalanche_c',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('avalanche_c', only_official=true, date_filter=true) }}
