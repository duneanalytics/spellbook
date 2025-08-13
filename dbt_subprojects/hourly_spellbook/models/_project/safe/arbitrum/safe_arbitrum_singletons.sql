{{ safe_table_config(
    blockchain = 'arbitrum',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('arbitrum', only_official=true, date_filter=true) }}
