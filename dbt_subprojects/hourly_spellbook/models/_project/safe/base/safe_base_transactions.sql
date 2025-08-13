{{ 
    safe_config(
        blockchain = 'base',
        alias_name = 'transactions'
    )
}}

{{ 
    safe_transactions(
        blockchain = 'base',
        project_start_date = '2023-07-01',
        date_filter = true
    ) 
}}
