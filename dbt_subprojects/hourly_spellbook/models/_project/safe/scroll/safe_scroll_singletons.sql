{{ safe_table_config(
    blockchain = 'scroll',
    alias_name = 'singletons'
) }}

{{ safe_singletons_by_network_validated('scroll', only_official=true, date_filter=true) }}
